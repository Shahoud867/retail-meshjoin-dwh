"""PeriodicThread (PDF 4.4 and 5.3): the simulated clock and the hourly group release.

It activates once every simulated hour and extracts the next group from the staging table
into the Stream buffer. Group 0 may go at startup, and every later group waits for its
hour. The clock itself never stops, whatever the mode, because periodic results load on
the tick and each hour closes with a log row.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass

from retail_dw.config import (
    PERIODIC_GROUPS,
    SPEED_NON_PEAK,
    TOPUP_INTERVAL_SECONDS,
    ConnectionConfig,
)
from retail_dw.control import RuntimeControl
from retail_dw.db import Database
from retail_dw.meshjoin.load_shedder import StagingGate
from retail_dw.meshjoin.model import PROCESSING_PERIODIC, Transaction
from retail_dw.meshjoin.stream_buffer import LANE_PERIODIC, StreamBuffer
from retail_dw.metrics import (
    ADMITTED,
    GROUP_SIZE,
    ITERATIONS,
    REINJECTED,
    SHED_OUT,
    HourStats,
    Metrics,
)

logger = logging.getLogger(__name__)

PERIODIC_WRITER = "periodic"

_SELECT_WAITING = (
    "SELECT staging_id, transaction_id, customer_id, product_id, quantity, unit_price, "
    "event_time, sales_mode FROM periodic_sales_data "
    "WHERE release_hour <= %s AND released = 0 ORDER BY staging_id LIMIT %s"
)
_SAVE_PROGRESS = (
    "INSERT INTO etl_progress (writer, next_group, sim_hour) VALUES (%s, %s, %s) "
    "ON DUPLICATE KEY UPDATE next_group = VALUES(next_group), sim_hour = VALUES(sim_hour)"
)


@dataclass(frozen=True)
class ClosedHour:
    """One finished simulated hour, handed to MeshJoinThread to load and log."""

    sim_hour: int
    speed_mode: str
    command_mode: str
    group_no: int | None
    stats: HourStats


class PeriodicThread(threading.Thread):
    """Runs the hour clock. Only peak or non-peak decides what happens to the overflow."""

    def __init__(
        self,
        cfg: ConnectionConfig,
        stream_buffer: StreamBuffer,
        control: RuntimeControl,
        metrics: Metrics,
        gate: StagingGate,
        closed_hours: queue.SimpleQueue[ClosedHour],
        seconds_per_hour: float,
    ) -> None:
        super().__init__(name="PeriodicThread")
        self._cfg = cfg
        self._buffer = stream_buffer
        self._control = control
        self._metrics = metrics
        self._gate = gate
        self._closed_hours = closed_hours
        self._seconds_per_hour = seconds_per_hour
        self._group_this_hour: int | None = None
        self._speed_at_hour_start = control.speed

    def run(self) -> None:
        try:
            with Database(self._cfg) as db:
                self._run_clock(db)
        except Exception as exc:  # noqa: BLE001 - any failure must stop the whole run
            logger.exception("PeriodicThread failed")
            self._control.fatal(exc)

    def _run_clock(self, db: Database) -> None:
        control = self._control
        self._open_next_group_and_admit(db)
        next_tick = time.monotonic() + self._seconds_per_hour
        while True:
            wait = min(TOPUP_INTERVAL_SECONDS, max(0.0, next_tick - time.monotonic()))
            if control.stop_readers.wait(wait):
                return
            now = time.monotonic()
            if now >= next_tick:
                next_tick += self._seconds_per_hour
                if next_tick <= now:
                    # A stall longer than an hour: one tick, with no burst of catch-up ticks.
                    next_tick = now + self._seconds_per_hour
                self._close_hour()
                control.advance_hour()
                self._speed_at_hour_start = control.speed
                self._group_this_hour = None
                db.run_transaction(self._save_progress, writer=PERIODIC_WRITER)
                self._open_next_group_and_admit(db)
            elif control.speed == SPEED_NON_PEAK and control.periodic_enabled():
                with self._gate.lock:
                    if self._gate.waiting > 0:
                        self._admit(db)
            self._note_if_done()

    def _close_hour(self) -> None:
        stats = self._metrics.take_hour()
        closed = ClosedHour(
            sim_hour=self._control.sim_hour,
            speed_mode=self._speed_at_hour_start,
            command_mode=self._control.mode,
            group_no=self._group_this_hour,
            stats=stats,
        )
        self._closed_hours.put(closed)
        logger.info(
            "Hour %d closed: speed=%s mode=%s group=%s group_size=%d admitted=%d shed=%d "
            "reinjected=%d iterations=%d",
            closed.sim_hour,
            closed.speed_mode,
            closed.command_mode,
            closed.group_no,
            stats[GROUP_SIZE],
            stats[ADMITTED],
            stats[SHED_OUT],
            stats[REINJECTED],
            stats[ITERATIONS],
        )

    def _open_next_group_and_admit(self, db: Database) -> None:
        """Open the group and admit what fits as one step, so that a group is never shed
        before it has had its chance at the buffer. `online` mode extracts nothing."""
        control = self._control
        if not control.periodic_enabled():
            return
        with self._gate.lock:
            group = control.open_next_group()
            if group is None:
                return
            self._group_this_hour = group
            size = db.run_transaction(
                lambda txn: self._open_group(txn, group), writer=PERIODIC_WRITER
            )
            self._gate.waiting += size
            self._metrics.add(GROUP_SIZE, size)
            self._admit(db)
        logger.info("Extracted group %d of %d", group, PERIODIC_GROUPS - 1)

    def _open_group(self, txn: Database, group: int) -> int:
        self._save_progress(txn)
        return txn.query(
            "SELECT COUNT(*) FROM periodic_sales_data WHERE release_hour = %s AND released = 0",
            (group,),
        )[0][0]

    def _save_progress(self, txn: Database) -> None:
        txn.execute(
            _SAVE_PROGRESS,
            (PERIODIC_WRITER, self._control.next_group, self._control.sim_hour),
        )

    def _admit(self, db: Database) -> int:
        """Move waiting rows into the buffer, as many as fit. The gate lock must be held.

        Slots are reserved before the rows are marked released, because a released row
        has nowhere else to go. The read takes no row locks: the gate already excludes the
        only other thread that could claim these rows.
        """
        slots = self._buffer.reserve_slots(self._gate.waiting)
        if slots == 0:
            return 0
        opened = self._control.opened_group
        try:
            rows = db.run_transaction(
                lambda txn: _claim_rows(txn, opened, slots), writer=PERIODIC_WRITER
            )
        except BaseException:
            self._buffer.release_slots(slots)
            raise
        now = time.monotonic()
        self._buffer.put_reserved(LANE_PERIODIC, [_periodic_transaction(r, now) for r in rows], slots)
        self._gate.waiting -= len(rows)
        self._metrics.add(ADMITTED, len(rows))
        return len(rows)

    def _note_if_done(self) -> None:
        if self._control.next_group >= PERIODIC_GROUPS and self._gate.waiting == 0:
            self._control.periodic_done.set()


def _claim_rows(txn: Database, opened_group: int, limit: int) -> list[tuple]:
    rows = txn.query(_SELECT_WAITING, (opened_group, limit))
    if rows:
        placeholders = ", ".join(["%s"] * len(rows))
        txn.execute(
            f"UPDATE periodic_sales_data SET released = 1 WHERE staging_id IN ({placeholders})",
            [row[0] for row in rows],
        )
    return rows


def _periodic_transaction(row: tuple, now: float) -> Transaction:
    staging_id, tid, customer, product, quantity, price, event_time, sales_mode = row
    return Transaction(
        tid,
        customer,
        product,
        quantity,
        price,
        event_time,
        sales_mode,
        PROCESSING_PERIODIC,
        staging_id=staging_id,
        enqueued_at=now,
    )
