"""The bonus cache (PDF section 16): Live results wait here for a slow period.

Rows are stored fully enriched, so a flush needs nothing from the join. A flush moves
them with the same Loader as every other path and deletes exactly the rows it copied,
all in one transaction, so a row is always in the cache or in the fact table and
never in both.
"""

from __future__ import annotations

from collections.abc import Sequence
from operator import attrgetter

from retail_dw.config import CACHE_FLUSH_SLICE
from retail_dw.db import Database
from retail_dw.load.consolidate import EnrichedRow, rebuild_row
from retail_dw.load.loader import MESHJOIN_WRITER, Loader
from retail_dw.meshjoin.model import PROCESSING_LIVE

# Named as the EnrichedRow fields, which is what lets a stored row be rebuilt from them.
_COLUMNS = (
    "transaction_id",
    "customer_id",
    "gender",
    "age_group",
    "occupation",
    "city_category",
    "stay_in_current_city_years",
    "marital_status",
    "product_id",
    "product_name",
    "product_category",
    "supplier_id",
    "supplier_name",
    "store_id",
    "event_time",
    "quantity",
    "revenue",
    "sales_mode",
    "processing_mode",
)
_COLUMN_LIST = ", ".join(_COLUMNS)
_values_of = attrgetter(*_COLUMNS)

_INSERT = (
    f"INSERT INTO live_cache ({_COLUMN_LIST}) "
    f"VALUES ({', '.join(['%s'] * len(_COLUMNS))})"
)
_SELECT_OLDEST = (
    f"SELECT cache_id, {_COLUMN_LIST} FROM live_cache ORDER BY cache_id LIMIT %s"
)


class LiveCache:
    """Owned by MeshJoinThread, whose session it uses, and the only writer of the table.

    The count is kept in memory because status reads it often, from any thread, and
    COUNT(*) would scan the whole table. It starts from the table, which is what makes
    it right after --resume, and only ever moves after a COMMIT has succeeded.
    """

    def __init__(self, db: Database) -> None:
        self._db = db
        self._count: int = db.query("SELECT COUNT(*) FROM live_cache")[0][0]

    def add(self, rows: Sequence[EnrichedRow], txn: Database) -> int:
        """Insert rows in the caller's open transaction, in order, and return how many.

        The whole batch is refused if any row is not Live, since periodic results must
        never be cached. The caller reports the rows with record_added once its commit
        has succeeded: this method may run again on a retry, or be rolled back.
        """
        for row in rows:
            if row.processing_mode != PROCESSING_LIVE:
                raise ValueError(
                    "Only Live rows may enter the cache, "
                    f"got {row.processing_mode!r} for {row.transaction_id!r}"
                )
        if rows:
            txn.executemany(_INSERT, [_values_of(row) for row in rows])
        return len(rows)

    def record_added(self, count: int) -> None:
        """Count rows that add() wrote, now that their transaction has committed."""
        self._count += count

    def count(self) -> int:
        """Committed rows in the cache."""
        return self._count

    def flush_slice(self, loader: Loader, limit: int = CACHE_FLUSH_SLICE) -> int:
        """Move up to limit of the oldest rows to the fact table and return how many.

        An empty cache returns at once, so that an idle loop writes nothing and its
        session cannot fail outside run_transaction.
        """
        if self._count == 0:
            return 0
        moved = self._db.run_transaction(
            lambda txn: _move_oldest(txn, loader, limit), writer=MESHJOIN_WRITER
        )
        self._count -= moved
        return moved


def _move_oldest(txn: Database, loader: Loader, limit: int) -> int:
    stored = txn.query(_SELECT_OLDEST, (limit,))
    if not stored:
        return 0
    loader.load(
        [rebuild_row(dict(zip(_COLUMNS, row[1:], strict=True))) for row in stored],
        tx=txn,
    )
    cache_ids = [row[0] for row in stored]
    placeholders = ", ".join(["%s"] * len(cache_ids))
    deleted = txn.execute(
        f"DELETE FROM live_cache WHERE cache_id IN ({placeholders})", cache_ids
    )
    if deleted != len(cache_ids):
        raise RuntimeError(
            f"Cache flush copied {len(cache_ids)} rows but deleted {deleted}"
        )
    return len(cache_ids)
