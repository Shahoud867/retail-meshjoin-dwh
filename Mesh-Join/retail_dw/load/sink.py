"""Where finished chunks go: Live now, Periodic and Shed on the hour, unmatched rejected.

Every method follows one rule. The database work runs inside run_transaction, which
may run it twice after a lost connection, and memory (the held list, the counters, the
cache count) changes only after it has returned. So a retry cannot count anything
twice, and a failed transaction leaves memory as it was.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence

from retail_dw.config import (
    CACHE_FLUSH_SLICE,
    LIVE_LOAD_CACHE,
    LIVE_LOADS,
    PERIODIC_LOAD_SLICE,
)
from retail_dw.db import Database
from retail_dw.load.consolidate import EnrichedRow, consolidate
from retail_dw.load.live_cache import LiveCache
from retail_dw.load.loader import MESHJOIN_WRITER, Loader
from retail_dw.meshjoin.model import PROCESSING_LIVE, PROCESSING_SHED, Chunk, Transaction
from retail_dw.metrics import (
    LIVE_COMMITTED,
    LIVE_LOADED,
    PERIODIC_LOADED,
    REJECTED,
    SHED_LOADED,
    Metrics,
)

# Values of periodic_sales_data.loaded. A rejected row is marked apart from a pending
# one, because --resume returns every released row with loaded = 0 to the stream and a
# rejected row would then be rejected a second time and break the row accounting.
STAGING_LOADED = 1
STAGING_REJECTED = 2

REASON_NO_CUSTOMER = "no customer match"
REASON_NO_PRODUCT = "no product match"
REASON_NO_MATCH = "no customer or product match"

_INSERT_REJECT = (
    "INSERT INTO etl_reject (transaction_id, customer_id, product_id, quantity, "
    "unit_price, event_time, sales_mode, processing_mode, reason) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)
_ADVANCE_ONLINE = (
    "UPDATE etl_progress SET online_rows_committed = online_rows_committed + %s "
    "WHERE writer = %s"
)


class StagingMismatchError(RuntimeError):
    """A load tried to finish staging rows that are missing or already finished.

    Raised inside the transaction, so the load rolls back. Going on would put a row in
    the warehouse that --resume could load again.
    """


def _reject_reason(tx: Transaction) -> str:
    if tx.customer is None and tx.product is None:
        return REASON_NO_MATCH
    return REASON_NO_CUSTOMER if tx.customer is None else REASON_NO_PRODUCT


def _reject_values(tx: Transaction) -> tuple:
    return (
        tx.transaction_id,
        tx.customer_id,
        tx.product_id,
        tx.quantity,
        tx.unit_price,
        tx.event_time,
        tx.sales_mode,
        tx.processing_mode,
        _reject_reason(tx),
    )


def _finish_staging(txn: Database, staging_ids: Sequence[int | None], state: int) -> None:
    """Record the outcome of staging rows, all of them or none. A row with no staging
    id matches nothing here, so it fails this check as well."""
    if not staging_ids:
        return
    placeholders = ", ".join(["%s"] * len(staging_ids))
    finished = txn.execute(
        "UPDATE periodic_sales_data SET loaded = %s "
        f"WHERE loaded = 0 AND staging_id IN ({placeholders})",
        (state, *staging_ids),
    )
    if finished != len(staging_ids):
        raise StagingMismatchError(
            f"Expected to finish {len(staging_ids)} staging rows, matched {finished}"
        )


class ResultSink:
    """Routes the chunks MESHJOIN has finished. Used by MeshJoinThread only, except
    that held_count may be read from any thread."""

    def __init__(
        self,
        db: Database,
        loader: Loader,
        live_cache: LiveCache,
        live_load: str,
        metrics: Metrics,
    ) -> None:
        if live_load not in LIVE_LOADS:
            raise ValueError(f"live load must be one of {LIVE_LOADS}")
        self._db = db
        self._loader = loader
        self._live_cache = live_cache
        self._live_load = live_load
        self._metrics = metrics
        self._held: list[EnrichedRow] = []

    @property
    def held_count(self) -> int:
        """Periodic and Shed results in memory, waiting for the hour tick."""
        return len(self._held)

    def handle(self, chunks: Iterable[Chunk], now: float | None = None) -> None:
        """Route finished chunks, which must be given oldest first.

        All Live rows of the call commit together with online_rows_committed, in file
        order, which is what makes that counter an exact resume offset. A rejected Live
        row counts too, since it has been dealt with and must not be read again.

        now is the commit time used for Live latency, on the time.monotonic scale. The
        clock is read after the commit when it is not given, which is the normal case.
        """
        live: list[EnrichedRow] = []
        live_arrivals: list[float] = []
        held: list[EnrichedRow] = []
        rejects: list[Transaction] = []
        online_rows = 0
        for chunk in chunks:
            for tx in chunk.transactions:
                is_live = tx.processing_mode == PROCESSING_LIVE
                online_rows += is_live
                if not tx.is_enriched:
                    rejects.append(tx)
                elif is_live:
                    live.append(consolidate(tx))
                    live_arrivals.append(tx.enqueued_at)
                else:
                    held.append(consolidate(tx))

        if live or rejects:
            self._db.run_transaction(
                lambda txn: self._commit_results(txn, live, rejects, online_rows),
                writer=MESHJOIN_WRITER,
            )
            self._count_results(live, live_arrivals, rejects, now)
        self._held.extend(held)

    def flush_held(self) -> int:
        """Load the held results in slices and return how many rows were loaded.

        Each slice is one transaction with its staging flags. If a slice fails, the
        slices before it stay loaded, and it and the later ones stay held.
        """
        loaded = 0
        while self._held:
            rows = self._held[:PERIODIC_LOAD_SLICE]
            self._load_held_slice(rows)
            del self._held[: len(rows)]
            loaded += len(rows)
        return loaded

    def flush_cache_slice(self, limit: int = CACHE_FLUSH_SLICE) -> int:
        """Move one slice from the cache to the fact table and return how many rows.

        Works in either live-load mode, so a run resumed with direct still empties a
        cache left behind by an earlier run in cache mode.
        """
        moved = self._live_cache.flush_slice(self._loader, limit)
        self._metrics.add(LIVE_LOADED, moved)
        return moved

    def _commit_results(
        self,
        txn: Database,
        live: Sequence[EnrichedRow],
        rejects: Sequence[Transaction],
        online_rows: int,
    ) -> None:
        if self._live_load == LIVE_LOAD_CACHE:
            self._live_cache.add(live, txn)
        else:
            self._loader.load(live, tx=txn)
        if rejects:
            txn.executemany(_INSERT_REJECT, [_reject_values(tx) for tx in rejects])
            _finish_staging(
                txn,
                [tx.staging_id for tx in rejects if tx.processing_mode != PROCESSING_LIVE],
                STAGING_REJECTED,
            )
        if online_rows:
            txn.execute(_ADVANCE_ONLINE, (online_rows, MESHJOIN_WRITER))

    def _count_results(
        self,
        live: Sequence[EnrichedRow],
        live_arrivals: Sequence[float],
        rejects: Sequence[Transaction],
        now: float | None,
    ) -> None:
        commit_time = time.monotonic() if now is None else now
        if self._live_load == LIVE_LOAD_CACHE:
            self._live_cache.record_added(len(live))
        else:
            self._metrics.add(LIVE_LOADED, len(live))
        self._metrics.add(LIVE_COMMITTED, len(live))
        self._metrics.add(REJECTED, len(rejects))
        self._metrics.record_live_latency(
            [commit_time - arrival for arrival in live_arrivals]
        )

    def _load_held_slice(self, rows: Sequence[EnrichedRow]) -> None:
        self._loader.load(
            rows,
            extra_in_txn=lambda txn: _finish_staging(
                txn, [row.staging_id for row in rows], STAGING_LOADED
            ),
        )
        shed = sum(row.processing_mode == PROCESSING_SHED for row in rows)
        self._metrics.add(SHED_LOADED, shed)
        self._metrics.add(PERIODIC_LOADED, len(rows) - shed)
