"""MySQL access: connection wrapper, recoverable transactions, script runner.

Every worker thread owns one Database. A Database is not safe to share between
threads, and that is what makes the commit recovery below sound: only one
connection ever raises a given writer's txn_seq.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from itertools import batched
from pathlib import Path
from types import TracebackType
from typing import Any, TypeVar

import mysql.connector
from mysql.connector import Error as MySqlError
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract

from retail_dw.config import ConnectionConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")

# InnoDB rolled back or refused the work, so running it again is safe.
DEADLOCK_ERROR = 1213
LOCK_WAIT_TIMEOUT_ERROR = 1205
LOCK_CONFLICT_ERRORS = frozenset({DEADLOCK_ERROR, LOCK_WAIT_TIMEOUT_ERROR})

# The session is gone. An open transaction died with it, unless COMMIT was already
# sent, in which case the client cannot tell what happened from the error alone.
SERVER_GONE_ERROR = 2006
LOST_DURING_QUERY_ERROR = 2013
LOST_SYSTEM_ERROR = 2055
LOST_CONNECTION_ERRORS = frozenset(
    {SERVER_GONE_ERROR, LOST_DURING_QUERY_ERROR, LOST_SYSTEM_ERROR}
)

_RETRY_BASE_DELAY_SECONDS = 0.05
_RETRY_MAX_DELAY_SECONDS = 1.0
_CONNECT_TIMEOUT_SECONDS = 10

# Set explicitly because a server configured without strict mode would quietly
# truncate an oversized value and the warehouse totals would be wrong with no error.
_SQL_MODE = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)

_DATABASE_NAME = re.compile(r"[A-Za-z0-9_]{1,64}")

_BUMP_TXN_SEQ = (
    "INSERT INTO etl_progress (writer, txn_seq) VALUES (%s, 1) "
    "ON DUPLICATE KEY UPDATE txn_seq = txn_seq + 1"
)


class CommitOutcomeUnknownError(RuntimeError):
    """A COMMIT may or may not have happened and there is no way to find out.

    Raised instead of guessing, because running the work again could load it twice
    and not running it could lose it.
    """


class SqlScriptError(ValueError):
    """A SQL script ends inside a quoted string or a block comment."""


def _open_connection(cfg: ConnectionConfig, *, select_database: bool = True) -> Any:
    """Open a session. Arguments are written out here, with no kwargs dict, so the
    password is not sitting in a local variable that a traceback could display.

    The pure Python driver is forced so that every install uses the code path the
    connection-loss tests ran against.
    """
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=(cfg.database or None) if select_database else None,
        charset="utf8mb4",
        autocommit=True,
        sql_mode=_SQL_MODE,
        use_pure=True,
        connection_timeout=_CONNECT_TIMEOUT_SECONDS,
    )


class Database:
    """One MySQL session in autocommit mode, with explicit transactions on demand."""

    def __init__(self, cfg: ConnectionConfig) -> None:
        self._cfg = cfg
        self._expected_seq: int | None = None
        self._open_session()

    def _open_session(self) -> None:
        self._conn: MySQLConnectionAbstract = _open_connection(self._cfg)
        # One cursor for the whole session. Asking the driver for a new cursor pings
        # the server first, which costs a round trip per statement and, on a dead
        # session, raises errno -1 in place of a real lost-connection code. Buffered,
        # so that no statement can leave an unread result behind.
        self._cursor: MySQLCursorAbstract = self._conn.cursor(buffered=True)
        # Cached because the driver cannot report it once the session has died,
        # which is exactly when a log line wants it.
        self._connection_id: int = self._conn.connection_id

    def __repr__(self) -> str:
        return (
            f"Database(host={self._cfg.host!r}, port={self._cfg.port!r}, "
            f"database={self._cfg.database!r}, connection_id={self._connection_id!r})"
        )

    def __enter__(self) -> Database:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @property
    def connection_id(self) -> int:
        """Server thread id of the current session, as used by KILL."""
        return self._connection_id

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
        """Run a statement and return its rows, or an empty list if it has none."""
        self._cursor.execute(sql, params)
        return self._cursor.fetchall() if self._cursor.with_rows else []

    def query_with_columns(
        self, sql: str, params: Sequence[Any] | None = None
    ) -> tuple[list[str], list[tuple]]:
        """Run a statement and return its column names and its rows."""
        rows = self.query(sql, params)
        return list(self._cursor.column_names), rows

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        """Run a statement and return the number of affected rows."""
        self._cursor.execute(sql, params)
        return self._cursor.rowcount

    def executemany(
        self, sql: str, rows: Iterable[Sequence[Any]], batch_size: int = 1000
    ) -> int:
        """Run a statement for many rows, in batches so that one call cannot exceed
        the server's max_allowed_packet. Returns the total affected rows."""
        total = 0
        for batch in batched(rows, batch_size):
            self._cursor.executemany(sql, list(batch))
            total += self._cursor.rowcount
        return total

    def reconnect(self) -> None:
        """Replace the session with a new one. Always a fresh connection, so that
        no state from the old session (autocommit, open transaction) can carry over.
        """
        self.close()
        self._open_session()

    def close(self) -> None:
        """Close the session. Safe to call twice or on a session that already died."""
        try:
            self._conn.close()
        except MySqlError as exc:
            logger.debug("Ignoring MySQL error %s while closing", exc.errno)

    @contextmanager
    def transaction(self, writer: str | None = None) -> Iterator[Database]:
        """Commit on success and roll back on any exception.

        With a writer, the transaction also raises that writer's etl_progress.txn_seq,
        which is what lets run_transaction find out whether a lost COMMIT landed.
        Nesting is refused by the driver: an inner commit that did not really commit
        would break the atomicity the caller is relying on.
        """
        self._conn.start_transaction()
        try:
            if writer is not None:
                self._expected_seq = self._bump_txn_seq(writer)
            yield self
            self._conn.commit()
        except BaseException:
            self._rollback_quietly()
            raise

    def run_transaction(
        self,
        fn: Callable[[Database], T],
        writer: str | None = None,
        retries: int = 3,
    ) -> T:
        """Run fn(self) in a transaction, surviving deadlocks and lost connections.

        Returns fn's result. fn may run more than once, so it must keep its side
        effects inside the transaction, or be able to repeat them. It must not
        commit by itself.

        Up to `retries` extra attempts are made after:
        - a deadlock or lock wait timeout (1213, 1205);
        - a connection lost before COMMIT was sent, because the server rolls back;
        - a connection lost at COMMIT that txn_seq shows did not land.

        If txn_seq shows that the lost COMMIT did land, fn is not run again and the
        result it returned before the COMMIT is returned. Without a writer a loss at
        COMMIT cannot be resolved, so CommitOutcomeUnknownError is raised. All other
        errors propagate immediately.
        """
        attempt = 0
        while True:
            fn_finished = False
            try:
                with self.transaction(writer):
                    result = fn(self)
                    fn_finished = True
                return result
            except MySqlError as exc:
                if exc.errno in LOST_CONNECTION_ERRORS:
                    self.reconnect()
                    if fn_finished:
                        if writer is None:
                            raise CommitOutcomeUnknownError(
                                "Connection lost at COMMIT with no writer to check"
                            ) from exc
                        if self._commit_landed(writer):
                            logger.warning(
                                "Connection lost at COMMIT for writer %s; "
                                "txn_seq shows the commit landed",
                                writer,
                            )
                            return result
                elif exc.errno not in LOCK_CONFLICT_ERRORS:
                    raise
                if attempt >= retries:
                    raise
                attempt += 1
                logger.warning(
                    "Retrying transaction after MySQL error %s (retry %d of %d)",
                    exc.errno,
                    attempt,
                    retries,
                )
                time.sleep(
                    min(_RETRY_BASE_DELAY_SECONDS * 2**attempt, _RETRY_MAX_DELAY_SECONDS)
                )

    def _bump_txn_seq(self, writer: str) -> int:
        """Raise the writer's sequence and return the value this transaction will
        leave behind if it commits."""
        self.execute(_BUMP_TXN_SEQ, (writer,))
        rows = self.query("SELECT txn_seq FROM etl_progress WHERE writer = %s", (writer,))
        return rows[0][0]

    def _commit_landed(self, writer: str) -> bool:
        """Decide whether the transaction that expected _expected_seq committed.

        This must be a locking read. The server can still be working on the lost
        COMMIT when the new session arrives. A plain read would see the old value and
        the work would run twice. The lost transaction holds the row lock from its
        txn_seq update until it has really finished, so FOR SHARE waits for the
        answer. If it waits too long, the outcome is reported as unknown.
        """
        try:
            rows = self.query(
                "SELECT txn_seq FROM etl_progress WHERE writer = %s FOR SHARE", (writer,)
            )
        except MySqlError as exc:
            if exc.errno in LOCK_CONFLICT_ERRORS:
                raise CommitOutcomeUnknownError(
                    f"Lost COMMIT for writer {writer} is still holding its lock"
                ) from exc
            raise
        return bool(rows) and rows[0][0] >= self._expected_seq

    def _rollback_quietly(self) -> None:
        """Roll back while another exception is propagating. If the session is dead
        the server has already rolled back, and the original error is the useful one.
        """
        try:
            self._conn.rollback()
        except MySqlError as exc:
            logger.debug("Ignoring MySQL error %s during rollback", exc.errno)


def validate_database_name(name: str) -> str:
    """Return the name if it is safe to quote into DDL, otherwise raise ValueError.

    Identifiers cannot be bound as parameters, so this check is what stands between
    typed input and the SQL text. fullmatch matters: with match and a "$" anchor, a
    name with a trailing newline would pass.
    """
    if not _DATABASE_NAME.fullmatch(name):
        raise ValueError(
            "Database name must be 1 to 64 characters: letters, digits and underscore"
        )
    return name


def ensure_database(cfg: ConnectionConfig) -> None:
    """Create the database in cfg if it does not exist yet."""
    name = validate_database_name(cfg.database)
    conn = _open_connection(cfg, select_database=False)
    try:
        cursor = conn.cursor(buffered=True)
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{name}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
        )
        cursor.close()
    finally:
        conn.close()


def locate_create_dw_script() -> Path:
    """Find Create-DW/create_dw.sql from the package location, so the program works
    from any working directory."""
    path = Path(__file__).resolve().parents[2] / "Create-DW" / "create_dw.sql"
    if not path.is_file():
        raise FileNotFoundError(f"Schema script not found: {path}")
    return path


def run_sql_script(db: Database, path: str | Path) -> int:
    """Run every statement of a script in order and return how many ran."""
    text = Path(path).read_text(encoding="utf-8-sig")
    statements = split_sql_statements(text)
    for statement in statements:
        db.execute(statement)
    return len(statements)


def split_sql_statements(text: str) -> list[str]:
    """Split a script into statements, following MySQL's own lexical rules.

    Comments are removed, except /*! ... */ and /*+ ... */ which the server executes.
    Empty statements are dropped. A final statement needs no semicolon. Raises
    SqlScriptError if the text ends inside a string or a block comment, because
    running half of a truncated script is worse than running none of it.
    """
    statements: list[str] = []
    current: list[str] = []

    def finish_statement() -> None:
        statement = "".join(current).strip()
        current.clear()
        if statement:
            statements.append(statement)

    position = 0
    while position < len(text):
        char = text[position]
        pair = text[position : position + 2]
        if char in "'\"`":
            end = _quoted_text_end(text, position)
            current.append(text[position:end])
            position = end
        elif pair == "/*":
            end = _block_comment_end(text, position)
            is_server_code = text[position + 2 : position + 3] in ("!", "+")
            # A space, because in MySQL a comment separates tokens: a/**/b is not ab.
            current.append(text[position:end] if is_server_code else " ")
            position = end
        elif char == "#" or (pair == "--" and _dash_comment_starts_at(text, position)):
            newline = text.find("\n", position)
            position = len(text) if newline < 0 else newline
        elif char == ";":
            finish_statement()
            position += 1
        else:
            current.append(char)
            position += 1
    finish_statement()
    return statements


def _dash_comment_starts_at(text: str, position: int) -> bool:
    """In MySQL "--" opens a comment only before whitespace, a control character or
    the end of the text. Without that rule, SELECT 5--1 would lose its tail."""
    following = text[position + 2 : position + 3]
    return following == "" or following <= " "


def _quoted_text_end(text: str, start: int) -> int:
    """Return the index just past the string or identifier opening at start. A doubled
    quote is a literal quote, and a backslash escapes only inside strings."""
    quote = text[start]
    position = start + 1
    while position < len(text):
        char = text[position]
        if char == "\\" and quote != "`":
            position += 2
        elif char != quote:
            position += 1
        elif text[position + 1 : position + 2] == quote:
            position += 2
        else:
            return position + 1
    raise SqlScriptError(f"Unterminated {quote} on line {_line_of(text, start)}")


def _block_comment_end(text: str, start: int) -> int:
    """Return the index just past the block comment opening at start."""
    close = text.find("*/", start + 2)
    if close < 0:
        raise SqlScriptError(f"Unterminated comment on line {_line_of(text, start)}")
    return close + 2


def _line_of(text: str, position: int) -> int:
    return text.count("\n", 0, position) + 1
