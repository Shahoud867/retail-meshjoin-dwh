"""Disk buffer and Master Data R as a cyclic source of partitions (PDF 4, 4.1 and 5.4).

Master Data R stays on disk for the whole run. One partition is one page of 500 tuples,
and R is traversed cyclically, in an endless loop. The engine only knows the small
PartitionSource interface, which keeps the join itself free of any database code.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, Protocol

from retail_dw.config import PARTITION_SIZE

if TYPE_CHECKING:
    from retail_dw.db import Database

# Table and column names cannot be bound as parameters, so they go into the SQL text and
# must be checked first. The check rejects quotes, backticks, dots, spaces and semicolons.
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class EmptyMasterDataError(ValueError):
    """A Master Data table has no rows, so no transaction could ever be enriched."""


class MasterDataChangedError(RuntimeError):
    """A partition does not have the size that the row count taken at the start promised.

    The partition count tells each chunk how long to stay in memory. If the table changed
    during the run, that count is wrong and a chunk could leave before seeing its match,
    so the run must stop and not lose rows silently.
    """


class PartitionSource(Protocol):
    """One table of Master Data R, seen as an endless cycle of partitions."""

    partitions: int
    """How many partitions make up one full cycle of the table."""

    position: int
    """1-based number of the partition read last, or 0 before the first read."""

    def next_partition(self) -> list[tuple]:
        """Return the next partition. After the last one, start again at the first."""
        ...


def _partition_count(row_count: int, size: int, name: str) -> int:
    """Count partitions from the rows, as the PDF asks. The last one may be short, and a
    table that divides exactly gets no empty partition after it."""
    if row_count == 0:
        raise EmptyMasterDataError(f"Master Data R table {name} has no rows")
    return math.ceil(row_count / size)


def _check_size(size: int) -> None:
    if size <= 0:
        raise ValueError("partition size must be greater than zero")


def _quote_identifier(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"Not a safe table or column name: {name!r}")
    return f"`{name}`"


class ListPartitionSource:
    """Master Data R held in a list, for tests of the DB-free engine. Never used by the
    program, since the PDF requires that R stays on disk."""

    def __init__(self, rows: Sequence[tuple], size: int = PARTITION_SIZE) -> None:
        _check_size(size)
        self._rows = rows
        self._size = size
        self.partitions = _partition_count(len(rows), size, "list")
        self.position = 0

    def next_partition(self) -> list[tuple]:
        self.position = self.position % self.partitions + 1
        start = (self.position - 1) * self._size
        return list(self._rows[start : start + self._size])


class MySqlPartitionSource:
    """One Master Data table read from MySQL, one page per call.

    Keyset pagination is used in place of OFFSET. OFFSET makes the server walk past every
    earlier row, while a key range goes straight to the page through the primary key, so
    the last page costs the same as the first. key_column must be the primary key, and
    must be one of the columns returned, because each page starts after the last key of
    the page before it.

    Nothing is cached. The only thing kept between calls is that one key. Every cycle
    reads the table again from disk, as the PDF requires.
    """

    def __init__(
        self,
        db: Database,
        table: str,
        key_column: str,
        columns: Sequence[str],
        size: int = PARTITION_SIZE,
    ) -> None:
        _check_size(size)
        if key_column not in columns:
            raise ValueError(f"key column {key_column} must be among the columns read")
        self._db = db
        self._table = table
        self._size = size
        self._key_index = list(columns).index(key_column)
        quoted_table = _quote_identifier(table)
        quoted_key = _quote_identifier(key_column)
        select_list = ", ".join(_quote_identifier(column) for column in columns)
        self._first_page_sql = (
            f"SELECT {select_list} FROM {quoted_table} ORDER BY {quoted_key} LIMIT {size}"
        )
        self._next_page_sql = (
            f"SELECT {select_list} FROM {quoted_table} WHERE {quoted_key} > %s "
            f"ORDER BY {quoted_key} LIMIT {size}"
        )
        self._row_count: int = db.query(f"SELECT COUNT(*) FROM {quoted_table}")[0][0]
        self.partitions = _partition_count(self._row_count, size, table)
        self.position = 0
        self._last_key: Any = None

    def next_partition(self) -> list[tuple]:
        """Read the next page. The position moves only after a successful read, so a
        failure can never skip a partition."""
        position = self.position % self.partitions + 1
        if position == 1:
            rows = self._read(self._first_page_sql, None)
        else:
            rows = self._read(self._next_page_sql, (self._last_key,))
        expected = self._expected_rows(position)
        if len(rows) != expected:
            raise MasterDataChangedError(
                f"Master Data R table {self._table}: partition {position} of "
                f"{self.partitions} has {len(rows)} rows, expected {expected}"
            )
        self._last_key = rows[-1][self._key_index]
        self.position = position
        return rows

    def _read(self, sql: str, params: tuple | None) -> list[tuple]:
        """Run one page read through run_transaction. A read can always be repeated, so
        a lost connection costs a reconnect and not the whole run. The one exception is a
        loss during the COMMIT of this read, which run_transaction reports as an unknown
        outcome. That stops the run in the controlled way, and --resume continues it."""
        return self._db.run_transaction(lambda db: db.query(sql, params))

    def _expected_rows(self, position: int) -> int:
        if position < self.partitions:
            return self._size
        return self._row_count - (self.partitions - 1) * self._size


class DiskBuffer:
    """Disk buffer of PDF 4.1: the one partition of R in memory, 500 tuples at most.

    Loading replaces the partition before it, so memory never grows with the size of R.
    """

    def __init__(self, capacity: int = PARTITION_SIZE) -> None:
        self._capacity = capacity
        self._rows: list[tuple] = []

    def load(self, rows: list[tuple]) -> None:
        """Replace the partition held. Raises ValueError if it does not fit."""
        if len(rows) > self._capacity:
            raise ValueError(
                f"Partition of {len(rows)} tuples exceeds the Disk buffer "
                f"size of {self._capacity}"
            )
        self._rows = rows

    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self) -> Iterator[tuple]:
        return iter(self._rows)
