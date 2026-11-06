"""Load shedding (PDF section 4.3): periodic rows go to disk at peak, and come back later.

A periodic group holds about 2,480 rows and the Stream buffer holds 1,000, so a released
group never fits at once. At peak speed MESHJOIN does not wait for it: whatever could not
enter the buffer moves from the staging table to the shed table. At non-peak speed nothing
is shed and those rows enter the buffer as room appears.

The move is one SQL statement from table to table, so shedding reads nothing into memory.
Only rows of periodic_sales_data can reach the shed table, which is what guarantees that
online rows are never shed.

Everything here runs in MeshJoinThread, as PDF section 4.4 requires.
"""

from __future__ import annotations

import threading
import time

from retail_dw.config import CHUNK_SIZE_W, HELD_PERIODIC_CAP
from retail_dw.control import RuntimeControl
from retail_dw.db import Database
from retail_dw.load.loader import MESHJOIN_WRITER
from retail_dw.meshjoin.model import PROCESSING_SHED, Transaction
from retail_dw.meshjoin.stream_buffer import LANE_SHED, StreamBuffer
from retail_dw.metrics import REINJECTED, SHED_OUT, Metrics

_TRANSACTION_COLUMNS = (
    "transaction_id, customer_id, product_id, quantity, unit_price, event_time, sales_mode"
)
_COPY_TO_SHED = (
    f"INSERT INTO shed_table (staging_id, {_TRANSACTION_COLUMNS}) "
    f"SELECT staging_id, {_TRANSACTION_COLUMNS} FROM periodic_sales_data "
    "WHERE release_hour <= %s AND released = 0 ORDER BY staging_id"
)
_MARK_SHED_RELEASED = (
    "UPDATE periodic_sales_data SET released = 1 WHERE release_hour <= %s AND released = 0"
)
_SELECT_OLDEST_SHED = (
    f"SELECT shed_id, staging_id, {_TRANSACTION_COLUMNS} FROM shed_table "
    "ORDER BY shed_id LIMIT %s"
)


class StagingGate:
    """The rows of opened groups that are still waiting in the staging table.

    PeriodicThread admits such rows and MeshJoinThread sheds them. Holding the lock across
    the whole database step makes those two exclusive, so a row can only take one route,
    and opening a group together with its first admission means a new group is never shed
    before it has had its chance at the buffer.
    """

    def __init__(self, waiting: int = 0) -> None:
        self.lock = threading.Lock()
        self.waiting = waiting


class LoadShedder:
    """The load shedding module of MeshJoinThread."""

    def __init__(
        self,
        db: Database,
        stream_buffer: StreamBuffer,
        control: RuntimeControl,
        metrics: Metrics,
        gate: StagingGate,
    ) -> None:
        self._db = db
        self._buffer = stream_buffer
        self._control = control
        self._metrics = metrics
        self._gate = gate
        # Started from the table, so it is right after --resume.
        self._on_disk: int = db.query("SELECT COUNT(*) FROM shed_table")[0][0]

    @property
    def on_disk(self) -> int:
        """Transactions still on the shed disk."""
        return self._on_disk

    def shed_overflow(self) -> int:
        """At peak speed, move the rows that did not fit the buffer to the shed table.

        `online` mode leaves them in staging, because that mode touches no periodic data.
        """
        if not (self._control.shed_allowed() and self._control.periodic_enabled()):
            return 0
        with self._gate.lock:
            if self._gate.waiting == 0:
                return 0
            opened = self._control.opened_group
            shed = self._db.run_transaction(
                lambda txn: _move_to_shed(txn, opened), writer=MESHJOIN_WRITER
            )
            self._gate.waiting = 0
        self._on_disk += shed
        self._metrics.add(SHED_OUT, shed)
        return shed

    def reinject_if_allowed(self, held_count: int) -> int:
        """At non-peak speed, bring shed rows back while MESHJOIN has room for them.

        They come last in the priority order, so they wait until no group rows are left
        and the buffer has room for a chunk. held_count stops them when too many joined
        periodic results are already waiting for the hour tick, since each slice held is
        memory the run cannot release until then.
        """
        if self._on_disk == 0 or not self._control.shed_processing_allowed():
            return 0
        if self._gate.waiting > 0 or held_count >= HELD_PERIODIC_CAP:
            return 0
        if self._buffer.counts().total >= CHUNK_SIZE_W:
            return 0
        slots = self._buffer.reserve_slots(CHUNK_SIZE_W)
        if slots == 0:
            return 0
        try:
            rows = self._db.run_transaction(
                lambda txn: _take_oldest_shed(txn, slots), writer=MESHJOIN_WRITER
            )
        except BaseException:
            self._buffer.release_slots(slots)
            raise
        now = time.monotonic()
        self._buffer.put_reserved(LANE_SHED, [_shed_transaction(row, now) for row in rows], slots)
        self._on_disk -= len(rows)
        self._metrics.add(REINJECTED, len(rows))
        return len(rows)


def _move_to_shed(txn: Database, opened_group: int) -> int:
    shed = txn.execute(_COPY_TO_SHED, (opened_group,))
    txn.execute(_MARK_SHED_RELEASED, (opened_group,))
    return shed


def _take_oldest_shed(txn: Database, limit: int) -> list[tuple]:
    rows = txn.query(_SELECT_OLDEST_SHED, (limit,))
    if rows:
        placeholders = ", ".join(["%s"] * len(rows))
        txn.execute(
            f"DELETE FROM shed_table WHERE shed_id IN ({placeholders})",
            [row[0] for row in rows],
        )
    return rows


def _shed_transaction(row: tuple, now: float) -> Transaction:
    _, staging_id, tid, customer, product, quantity, price, event_time, sales_mode = row
    return Transaction(
        tid,
        customer,
        product,
        quantity,
        price,
        event_time,
        sales_mode,
        PROCESSING_SHED,
        staging_id=staging_id,
        enqueued_at=now,
    )
