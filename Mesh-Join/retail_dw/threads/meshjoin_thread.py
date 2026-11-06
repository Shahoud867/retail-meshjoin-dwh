"""MeshJoinThread (PDF 4.4): runs MESHJOIN and load shedding, and loads the warehouse.

It is the only writer of the dimensions and the fact table, which is why two loads can
never race to insert the same dimension value. There is no separate loader thread, since
the PDF asks for exactly three.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from retail_dw.config import (
    CHUNK_SIZE_W,
    MIN_ITERATION_SECONDS,
    ConnectionConfig,
)
from retail_dw.control import RuntimeControl
from retail_dw.db import CommitOutcomeUnknownError, Database, MySqlError
from retail_dw.load.live_cache import LiveCache
from retail_dw.load.loader import MESHJOIN_WRITER, Loader
from retail_dw.load.sink import ResultSink
from retail_dw.meshjoin.disk_buffer import MySqlPartitionSource
from retail_dw.meshjoin.engine import MeshJoinEngine
from retail_dw.meshjoin.load_shedder import LoadShedder, StagingGate
from retail_dw.meshjoin.stream_buffer import StreamBuffer
from retail_dw.metrics import (
    ADMITTED,
    GROUP_SIZE,
    ITERATIONS,
    LIVE_COMMITTED,
    LIVE_LOADED,
    PERIODIC_LOADED,
    REINJECTED,
    REJECTED,
    SHED_LOADED,
    SHED_OUT,
    Metrics,
)
from retail_dw.threads.periodic_thread import ClosedHour

logger = logging.getLogger(__name__)

CUSTOMER_COLUMNS = (
    "customer_id",
    "gender",
    "age_group",
    "occupation",
    "city_category",
    "stay_in_current_city_years",
    "marital_status",
)
PRODUCT_COLUMNS = (
    "product_id",
    "product_name",
    "product_category",
    "supplier_id",
    "supplier_name",
)

_IDLE_WAIT_SECONDS = 0.05
_STATUS_INTERVAL_SECONDS = 1.0

_INSERT_HOUR = (
    "INSERT INTO etl_hourly_log (sim_hour, speed_mode, command_mode, group_no, group_size, "
    "admitted, shed_out, reinjected, periodic_loaded, shed_loaded, live_committed, "
    "on_shed_disk, on_live_cache, fact_rows, live_latency_p50_ms, live_latency_p95_ms, "
    "iterations, avg_iteration_ms) VALUES "
    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
_STATUS_COLUMNS = (
    "command_mode",
    "speed_mode",
    "current_tps",
    "sim_hour",
    "stream_buffer",
    "stream_online",
    "stream_periodic",
    "stream_shed",
    "hash_table",
    "queue_chunks",
    "customer_partition",
    "customer_partitions",
    "product_partition",
    "product_partitions",
    "on_shed_disk",
    "on_live_cache",
    "held_periodic",
    "fact_rows",
    "live_load",
    "state",
)
_UPSERT_STATUS = (
    f"INSERT INTO etl_live_status (id, {', '.join(_STATUS_COLUMNS)}) "
    f"VALUES (1, {', '.join(['%s'] * len(_STATUS_COLUMNS))}) ON DUPLICATE KEY UPDATE "
    + ", ".join(f"{column} = VALUES({column})" for column in _STATUS_COLUMNS)
)


@dataclass(frozen=True)
class PipelineStatus:
    """One reading of the pipeline, in the PDF's component names, for `status`."""

    mode: str
    speed: str
    tps: int
    sim_hour: int
    groups_released: int
    stream_buffer: int
    stream_limit: int
    stream_online: int
    stream_periodic: int
    stream_shed: int
    hash_table: int
    queue_chunks: int
    customer_partition: int
    customer_partitions: int
    product_partition: int
    product_partitions: int
    on_shed_disk: int
    on_live_cache: int
    held_periodic: int
    fact_rows: int
    live_load: str
    state: str


class MeshJoinThread(threading.Thread):
    """Built on the main thread, so that a bad Master Data table stops the program before
    any thread starts. From start() on, only this thread uses its database session."""

    def __init__(
        self,
        cfg: ConnectionConfig,
        stream_buffer: StreamBuffer,
        control: RuntimeControl,
        metrics: Metrics,
        gate: StagingGate,
        closed_hours: queue.SimpleQueue[ClosedHour],
        live_load: str,
        expected_rows: int,
        out: Callable[[str], object] = print,
    ) -> None:
        super().__init__(name="MeshJoinThread")
        self._control = control
        self._metrics = metrics
        self._buffer = stream_buffer
        self._gate = gate
        self._closed_hours = closed_hours
        self._live_load = live_load
        self._expected_rows = expected_rows
        self._out = out
        self.readers_finished = threading.Event()

        self._db = Database(cfg)
        self.engine = MeshJoinEngine(
            stream_buffer,
            MySqlPartitionSource(
                self._db, "customer_master_data", "customer_id", CUSTOMER_COLUMNS
            ),
            MySqlPartitionSource(self._db, "product_master_data", "product_id", PRODUCT_COLUMNS),
        )
        self._live_cache = LiveCache(self._db)
        self._sink = ResultSink(self._db, Loader(self._db), self._live_cache, live_load, metrics)
        self._shedder = LoadShedder(self._db, stream_buffer, control, metrics, gate)
        self._base_fact_rows: int = self._db.query("SELECT COUNT(*) FROM fact_sales")[0][0]
        self._base_rejects: int = self._db.query("SELECT COUNT(*) FROM etl_reject")[0][0]
        self._state = "starting"
        self._next_status = 0.0
        self._announced_complete = False
        self._announced_hint = False

    def run(self) -> None:
        try:
            self._state = "running"
            self._run_until_stopped()
            if not self._control.stop_all.is_set():
                self._drain()
            self._state = "stopped"
            self._publish_status()
        except Exception as exc:  # noqa: BLE001 - any failure must stop the whole run
            logger.exception("MeshJoinThread failed")
            self._state = "failed"
            self._control.fatal(exc)
        finally:
            self._db.close()

    def status(self) -> PipelineStatus:
        """Safe to call from the console thread. Values can be one iteration old."""
        control = self._control
        counts = self._buffer.counts()
        engine = self.engine
        return PipelineStatus(
            mode=control.mode,
            speed=control.speed,
            tps=control.current_tps(),
            sim_hour=control.sim_hour,
            groups_released=control.next_group,
            stream_buffer=counts.total,
            stream_limit=self._buffer.limit,
            stream_online=counts.online,
            stream_periodic=counts.periodic,
            stream_shed=counts.shed,
            hash_table=engine.h_size,
            queue_chunks=engine.queue_len,
            customer_partition=engine.customer_position,
            customer_partitions=engine.customer_partitions,
            product_partition=engine.product_position,
            product_partitions=engine.product_partitions,
            on_shed_disk=self._shedder.on_disk,
            on_live_cache=self._live_cache.count(),
            held_periodic=self._sink.held_count,
            fact_rows=self._fact_rows(),
            live_load=self._live_load,
            state=self._state,
        )

    def _fact_rows(self) -> int:
        """Kept from the counters, because COUNT(*) every second would scan the table."""
        metrics = self._metrics
        return (
            self._base_fact_rows
            + metrics.total(LIVE_LOADED)
            + metrics.total(PERIODIC_LOADED)
            + metrics.total(SHED_LOADED)
        )

    def _run_until_stopped(self) -> None:
        control = self._control
        while not (control.stop_readers.is_set() or control.stop_all.is_set()):
            started = time.monotonic()
            self._housekeeping()
            worked = self._iterate_once()
            self._publish_status_if_due()
            self._announce_progress()
            self._pace(started, worked)

    def _housekeeping(self) -> None:
        """Everything between iterations. The four steps themselves stay as the PDF has them."""
        self._shedder.shed_overflow()
        self._shedder.reinject_if_allowed(self._sink.held_count)
        self._load_closed_hours()
        if self._control.cache_flush_allowed():
            self._sink.flush_cache_slice()

    def _iterate_once(self) -> bool:
        if not self.engine.has_work():
            return False
        started = time.monotonic()
        finished = self.engine.run_iteration()
        self._sink.handle(finished)
        self._metrics.record_iteration(time.monotonic() - started)
        return True

    def _load_closed_hours(self) -> None:
        """The hour has ticked, so the periodic results it joined load now (PDF section 7)."""
        while True:
            try:
                closed = self._closed_hours.get_nowait()
            except queue.Empty:
                return
            self._sink.flush_held()
            self._write_hour_row(closed)

    def _write_hour_row(self, closed: ClosedHour) -> None:
        stats = closed.stats
        values = (
            closed.sim_hour,
            closed.speed_mode,
            closed.command_mode,
            closed.group_no,
            stats[GROUP_SIZE],
            stats[ADMITTED],
            stats[SHED_OUT],
            stats[REINJECTED],
            stats[PERIODIC_LOADED],
            stats[SHED_LOADED],
            stats[LIVE_COMMITTED],
            self._shedder.on_disk,
            self._live_cache.count(),
            self._fact_rows(),
            stats.live_latency_p50_ms,
            stats.live_latency_p95_ms,
            stats[ITERATIONS],
            round(stats.avg_iteration_ms, 2),
        )
        self._db.run_transaction(
            lambda txn: txn.execute(_INSERT_HOUR, values), writer=MESHJOIN_WRITER
        )

    def _pace(self, started: float, worked: bool) -> None:
        """A chunk's worth of waiting rows runs straight away. Otherwise the pause lets rows
        gather, so a slow stream does not read 1,000 Master Data tuples for a handful of
        transactions many times a second."""
        if not worked:
            self._control.stop_all.wait(_IDLE_WAIT_SECONDS)
            return
        if self._buffer.counts().total >= CHUNK_SIZE_W:
            return
        remaining = MIN_ITERATION_SECONDS - (time.monotonic() - started)
        if remaining > 0:
            self._control.stop_all.wait(remaining)

    def _drain(self) -> None:
        """After quit, finish what is already in memory. Rows on disk stay there, so a
        later --resume carries on from them. Cached Live rows all load, because the end of
        a run is the last slow period it will have."""
        self._state = "draining"
        self.readers_finished.wait()
        while self.engine.has_work() and not self._control.stop_all.is_set():
            self._iterate_once()
        self._load_closed_hours()
        self._sink.flush_held()
        while self._sink.flush_cache_slice():
            pass
        final = self._metrics.take_hour()
        self._write_hour_row(
            ClosedHour(
                sim_hour=self._control.sim_hour,
                speed_mode=self._control.speed,
                command_mode=self._control.mode,
                group_no=None,
                stats=final,
            )
        )

    def _publish_status_if_due(self) -> None:
        now = time.monotonic()
        if now >= self._next_status:
            self._next_status = now + _STATUS_INTERVAL_SECONDS
            self._publish_status()

    def _publish_status(self) -> None:
        """For watching from outside. Losing one update does not matter, so a failure here
        is logged and the join carries on."""
        status = self.status()
        values = tuple(
            {
                "command_mode": status.mode,
                "speed_mode": status.speed,
                "current_tps": status.tps,
                "sim_hour": status.sim_hour,
                "stream_buffer": status.stream_buffer,
                "stream_online": status.stream_online,
                "stream_periodic": status.stream_periodic,
                "stream_shed": status.stream_shed,
                "hash_table": status.hash_table,
                "queue_chunks": status.queue_chunks,
                "customer_partition": status.customer_partition,
                "customer_partitions": status.customer_partitions,
                "product_partition": status.product_partition,
                "product_partitions": status.product_partitions,
                "on_shed_disk": status.on_shed_disk,
                "on_live_cache": status.on_live_cache,
                "held_periodic": status.held_periodic,
                "fact_rows": status.fact_rows,
                "live_load": status.live_load,
                "state": status.state,
            }[column]
            for column in _STATUS_COLUMNS
        )
        try:
            self._db.run_transaction(
                lambda txn: txn.execute(_UPSERT_STATUS, values), writer=MESHJOIN_WRITER
            )
        except (MySqlError, CommitOutcomeUnknownError) as exc:
            logger.warning("Could not publish the live status: %s", type(exc).__name__)

    def _announce_progress(self) -> None:
        if self._announced_complete:
            return
        rejects = self._base_rejects + self._metrics.total(REJECTED)
        fact_rows = self._fact_rows()
        if fact_rows + rejects >= self._expected_rows:
            self._announced_complete = True
            self._out(
                f"ALL DATA LOADED: {fact_rows:,} fact rows, {rejects:,} rejected. "
                "The warehouse is ready for the queries. Type quit to close the program."
            )
            return
        control = self._control
        streams_done = control.online_done.is_set() and control.periodic_done.is_set()
        leftover = self._shedder.on_disk > 0 or self._live_cache.count() > 0
        if (
            streams_done
            and leftover
            and control.shed_allowed()
            and not self.engine.has_work()
            and not self._announced_hint
        ):
            self._announced_hint = True
            self._out(
                "Both streams have finished. Shed or cached rows are still waiting: "
                "type non-peak to process them."
            )
