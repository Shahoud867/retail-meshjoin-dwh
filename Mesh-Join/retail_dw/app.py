"""The program, in the order of PDF section 9: create the tables, prepare the data, start the
three threads, load, and keep accepting commands until it is closed."""

from __future__ import annotations

import logging
import queue
import re
import sys
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from retail_dw.config import ConnectionConfig, Settings, parse_settings
from retail_dw.console import (
    StartupAborted,
    command_loop,
    format_status,
    print_banner,
    prompt_connection,
    prompt_paths,
    read_password,
)
from retail_dw.control import RuntimeControl
from retail_dw.db import (
    CommitOutcomeUnknownError,
    Database,
    MySqlError,
    ensure_database,
    locate_create_dw_script,
    run_sql_script,
)
from retail_dw.meshjoin.load_shedder import StagingGate
from retail_dw.meshjoin.stream_buffer import StreamBuffer
from retail_dw.metrics import ONLINE_BAD_LINES, Metrics
from retail_dw.reconcile import print_account, take_account
from retail_dw.threads.meshjoin_thread import MeshJoinThread
from retail_dw.threads.online_thread import OnlineThread, count_online_rows
from retail_dw.threads.periodic_thread import ClosedHour, PeriodicThread

logger = logging.getLogger(__name__)

_ACCESS_DENIED = 1045
_MAX_LOGIN_ATTEMPTS = 3
_DROP_TABLE = re.compile(r"^\s*DROP\s+TABLE\s+IF\s+EXISTS\s+(\w+)\s*;", re.I | re.M)


class StartupError(RuntimeError):
    """The program cannot start. The message says what to do about it."""


@dataclass(frozen=True)
class RunStart:
    """Where the threads begin, for a fresh run or a resumed one."""

    online_offset: int
    sim_hour: int
    next_group: int
    staging_waiting: int
    expected_rows: int
    group_min: int | None
    group_max: int | None


def _print(message: str) -> None:
    print(message, flush=True)


def _configure_logging(log_dir: Path) -> Path:
    """Logs go to a file, so the console stays usable for typing commands."""
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"etl_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        filename=path,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("mysql.connector").setLevel(logging.WARNING)
    return path


def _project_tables_in_script(script: Path) -> set[str]:
    return {name.lower() for name in _DROP_TABLE.findall(script.read_text(encoding="utf-8"))}


def foreign_tables_at_risk(db: Database, script: Path) -> list[str]:
    """Tables the script would drop in a database this project did not create.

    Another project on the server can use the same names, such as fact_sales. A database
    with etl_progress was made by this program, so dropping its tables is what the PDF
    asks for.
    """
    rows = db.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
    )
    tables = {row[0].lower() for row in rows}
    if "etl_progress" in tables:
        return []
    return sorted(tables & _project_tables_in_script(script))


def _confirm_drop(cfg: ConnectionConfig, at_risk: list[str]) -> None:
    listing = ", ".join(at_risk)
    message = (
        f"Database {cfg.database} was not created by this program, and it holds tables "
        f"this program would drop: {listing}."
    )
    if not sys.stdin.isatty():
        raise StartupError(f"{message} Choose another database name.")
    _print(message)
    _print(f"Type the database name ({cfg.database}) to drop them, or press Enter to stop:")
    if sys.stdin.readline().strip() != cfg.database:
        raise StartupError("Stopped. Nothing was changed. Choose another database name.")


def _connect_with_retries(cfg: ConnectionConfig) -> ConnectionConfig:
    """A wrong password can be typed again on a terminal. A pipe cannot correct itself."""
    for attempt in range(1, _MAX_LOGIN_ATTEMPTS + 1):
        try:
            ensure_database(cfg)
            return cfg
        except MySqlError as exc:
            if exc.errno != _ACCESS_DENIED:
                raise StartupError(
                    f"Could not reach MySQL at {cfg.host}:{cfg.port} (error {exc.errno}). "
                    "Check that the server is running and the host and port are right."
                ) from exc
            _print("Access denied: check the user name and password.")
            if attempt == _MAX_LOGIN_ATTEMPTS or not sys.stdin.isatty():
                break
            cfg = ConnectionConfig(cfg.host, cfg.port, cfg.database, cfg.user, read_password())
    raise StartupError("Could not log in to MySQL.")


def _start_fresh(
    settings: Settings, cfg: ConnectionConfig, online_path: Path, raw_path: Path
) -> RunStart:
    # Imported here so that a resumed run never needs pandas.
    from retail_dw.prepare.fetch import ensure_raw_file
    from retail_dw.prepare.pipeline import prepare_all

    script = locate_create_dw_script()
    with Database(cfg) as db:
        at_risk = foreign_tables_at_risk(db, script)
        if at_risk:
            _confirm_drop(cfg, at_risk)
        raw = ensure_raw_file(raw_path, _confirm_download)
        _print("Creating the working tables and the warehouse...")
        run_sql_script(db, script)
    _print("Reading and cleaning the transactional file...")
    summary = prepare_all(settings, cfg, raw, online_path, out=_print)
    return RunStart(0, 0, 0, 0, summary.after, summary.group_min, summary.group_max)


def _confirm_download(message: str) -> bool:
    if not sys.stdin.isatty():
        _print(message)
        return False
    _print(f"{message} [y/N]")
    return sys.stdin.readline().strip().lower() == "y"


def _start_resumed(cfg: ConnectionConfig, online_path: Path) -> RunStart:
    if not online_path.is_file():
        raise StartupError(f"--resume needs the online file, which is not at {online_path}")
    with Database(cfg) as db:
        tables = db.query(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = 'etl_progress'"
        )
        prepared = (
            db.query("SELECT prepared FROM etl_progress WHERE writer = 'prepare'")
            if tables
            else []
        )
        if not (prepared and prepared[0][0] == 1):
            raise StartupError(
                f"Database {cfg.database} has no finished data preparation to resume. "
                "Start without --resume."
            )
        # These rows were in memory when the run stopped, so they run through the join again.
        returned = db.run_transaction(
            lambda txn: txn.execute(
                "UPDATE periodic_sales_data p "
                "LEFT JOIN shed_table s ON s.staging_id = p.staging_id "
                "SET p.released = 0 "
                "WHERE p.released = 1 AND p.loaded = 0 AND s.staging_id IS NULL"
            ),
            writer="meshjoin",
        )
        committed_rows = db.query(
            "SELECT online_rows_committed FROM etl_progress WHERE writer = 'meshjoin'"
        )
        clock_rows = db.query(
            "SELECT sim_hour, next_group FROM etl_progress WHERE writer = 'periodic'"
        )
        committed = committed_rows[0][0] if committed_rows else 0
        sim_hour, next_group = clock_rows[0] if clock_rows else (0, 0)
        waiting = db.query(
            "SELECT COUNT(*) FROM periodic_sales_data WHERE release_hour < %s AND released = 0",
            (next_group,),
        )[0][0]
        staged = db.query("SELECT COUNT(*) FROM periodic_sales_data")[0][0]
    _print(
        f"Resuming: {committed:,} online rows already committed, {returned:,} periodic rows "
        f"returned to staging, next group {next_group}, simulated hour {sim_hour}."
    )
    expected = count_online_rows(online_path) + staged
    return RunStart(committed, sim_hour, next_group, waiting, expected, None, None)


def _run_pipeline(
    settings: Settings, cfg: ConnectionConfig, online_path: Path, start: RunStart
) -> int:
    control = RuntimeControl(settings, sim_hour=start.sim_hour, next_group=start.next_group)
    control.set_fatal_listener(
        lambda error: _print(
            f"ERROR: the run is stopping ({_describe(error)}). Press Enter to close the program."
        )
    )
    metrics = Metrics()
    stream_buffer = StreamBuffer()
    gate = StagingGate(start.staging_waiting)
    closed_hours: queue.SimpleQueue[ClosedHour] = queue.SimpleQueue()

    meshjoin = MeshJoinThread(
        cfg,
        stream_buffer,
        control,
        metrics,
        gate,
        closed_hours,
        settings.live_load,
        start.expected_rows,
        out=_print,
    )
    online = OnlineThread(online_path, stream_buffer, control, metrics, start.online_offset)
    periodic = PeriodicThread(
        cfg, stream_buffer, control, metrics, gate, closed_hours, settings.seconds_per_hour
    )
    print_banner(
        settings,
        control,
        meshjoin.engine.customer_partitions,
        meshjoin.engine.product_partitions,
        start.group_min,
        start.group_max,
        out=_print,
    )
    meshjoin.start()
    online.start()
    periodic.start()
    logger.info("Started OnlineThread, MeshJoinThread and PeriodicThread")

    command_loop(control, lambda: format_status(meshjoin.status()), out=_print)

    _print("Stopping the readers and finishing what is in memory...")
    control.stop_readers.set()
    online.join()
    periodic.join()
    meshjoin.readers_finished.set()
    meshjoin.join()
    logger.info("All threads have stopped")

    with Database(cfg) as db:
        print_account(take_account(db, str(online_path), metrics.total(ONLINE_BAD_LINES)), _print)

    error = control.fatal_error
    if error is not None:
        _print(f"The run stopped because of an error: {_describe(error)}")
        _print("Rows on disk are safe. Fix the cause, then start again with --resume.")
        return 1
    _print("Closed.")
    return 0


def _describe(error: BaseException) -> str:
    """The type and the MySQL code. Never str(error) on a connect failure, which could
    carry details of the connection."""
    if isinstance(error, MySqlError):
        return f"MySQL error {error.errno}"
    if isinstance(error, CommitOutcomeUnknownError):
        return "the connection was lost during a commit"
    return f"{type(error).__name__}: {error}"


def main(argv: Sequence[str] | None = None) -> int:
    settings = parse_settings(argv)
    threading.current_thread().name = "Console"
    log_path = _configure_logging(settings.log_dir)
    try:
        cfg = _connect_with_retries(prompt_connection())
        online_path, raw_path = prompt_paths()
        if settings.resume:
            start = _start_resumed(cfg, online_path)
        else:
            start = _start_fresh(settings, cfg, online_path, raw_path)
        return _run_pipeline(settings, cfg, online_path, start)
    except (StartupError, StartupAborted, ValueError, OSError) as exc:
        logger.error("Startup failed: %s", exc)
        _print(str(exc))
        return 1
    except MySqlError as exc:
        logger.error("MySQL error %s", exc.errno)
        _print(f"MySQL error {exc.errno}. See {log_path}.")
        return 1
