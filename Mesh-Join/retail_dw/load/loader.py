"""Load consolidated rows into the star schema (PDF section 7).

A dimension value that is not yet in its table goes in first, then the fact rows that
need it, all in one transaction. Existing values are found with a SELECT and never
sent to the server as inserts, because a refused or ignored AUTO_INCREMENT insert
still uses up a surrogate key. Key maps live for one batch only: holding every id in a
long-lived dictionary would be the in-memory copy of the dimensions that a warehouse
this size should not need.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from itertools import batched
from operator import attrgetter
from typing import Any

from retail_dw.db import Database
from retail_dw.load.consolidate import EnrichedRow

# Every transaction that writes dimensions, facts, the cache or the Live progress
# belongs to this writer, whose txn_seq resolves a connection lost at COMMIT.
MESHJOIN_WRITER = "meshjoin"

_LOOKUP_BATCH = 1000

_TIME_COLUMNS = (
    "time_key",
    "full_datetime",
    "full_date",
    "hour_of_day",
    "day_of_month",
    "month_no",
    "month_name",
    "quarter_no",
    "year_no",
    "day_of_week",
    "day_name",
    "day_type",
    "season",
    "half_year",
)


def _time_values(row: EnrichedRow) -> tuple[Any, ...]:
    return tuple(row.time[column] for column in _TIME_COLUMNS)


@dataclass(frozen=True)
class _Dimension:
    """One dimension table. The natural id is the first of its columns."""

    table: str
    key_column: str
    columns: tuple[str, ...]
    values_of: Callable[[EnrichedRow], tuple[Any, ...]]

    @property
    def natural_column(self) -> str:
        return self.columns[0]

    @property
    def insert_sql(self) -> str:
        placeholders = ", ".join(["%s"] * len(self.columns))
        return (
            f"INSERT INTO {self.table} ({', '.join(self.columns)}) "
            f"VALUES ({placeholders})"
        )


def _dimension(table: str, key_column: str, *columns: str) -> _Dimension:
    return _Dimension(table, key_column, columns, attrgetter(*columns))


_CUSTOMER = _dimension(
    "dim_customer",
    "customer_key",
    "customer_id",
    "gender",
    "age_group",
    "occupation",
    "city_category",
    "stay_in_current_city_years",
    "marital_status",
)
_PRODUCT = _dimension(
    "dim_product", "product_key", "product_id", "product_name", "product_category"
)
_SUPPLIER = _dimension("dim_supplier", "supplier_key", "supplier_id", "supplier_name")
_STORE = _dimension("dim_store", "store_key", "store_id", "store_name", "city")
# The key is built from the value itself, so it is also the natural id.
_TIME = _Dimension("dim_time", "time_key", _TIME_COLUMNS, _time_values)

_INSERT_FACT = (
    "INSERT INTO fact_sales (customer_key, product_key, supplier_key, store_key, "
    "time_key, transaction_id, sales_mode, processing_mode, quantity, revenue) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)


def _select_keys(
    txn: Database, dimension: _Dimension, naturals: Iterable[Any]
) -> dict[Any, Any]:
    """Map natural ids to surrogate keys for those of the ids that exist."""
    sql = (
        f"SELECT {dimension.natural_column}, {dimension.key_column} "
        f"FROM {dimension.table} WHERE {dimension.natural_column} IN "
    )
    keys: dict[Any, Any] = {}
    for part in batched(naturals, _LOOKUP_BATCH):
        placeholders = ", ".join(["%s"] * len(part))
        keys.update(txn.query(f"{sql}({placeholders})", part))
    return keys


def _resolve_keys(
    txn: Database, dimension: _Dimension, rows: Sequence[EnrichedRow]
) -> dict[Any, Any]:
    """Return the key of every id in the batch, inserting only the missing values.

    The natural-id comparison uses the column's collation, utf8mb4_bin for ids, so
    85123A and 85123a are different products here as well as in the dictionary.
    """
    wanted: dict[Any, tuple[Any, ...]] = {}
    for row in rows:
        values = dimension.values_of(row)
        wanted.setdefault(values[0], values)
    keys = _select_keys(txn, dimension, wanted)
    missing = [values for natural, values in wanted.items() if natural not in keys]
    if missing:
        txn.executemany(dimension.insert_sql, missing)
        keys.update(_select_keys(txn, dimension, (values[0] for values in missing)))
    return keys


class Loader:
    """Writes dimensions and facts. MeshJoinThread is their only writer, so a value
    cannot appear between the SELECT that misses it and the INSERT that adds it."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def load(
        self,
        rows: Iterable[EnrichedRow],
        extra_in_txn: Callable[[Database], Any] | None = None,
        tx: Database | None = None,
    ) -> int:
        """Load rows as one transaction and return the number of fact rows.

        extra_in_txn runs after the fact insert, in the same transaction, so a caller
        can commit its own bookkeeping together with the rows. It may run more than
        once. With tx the work joins that open transaction, which its owner commits;
        without it the load is its own run_transaction, safe against a lost connection.
        """
        batch = list(rows)
        if not batch and extra_in_txn is None:
            return 0

        def work(txn: Database) -> int:
            _write_rows(txn, batch)
            if extra_in_txn is not None:
                extra_in_txn(txn)
            return len(batch)

        if tx is not None:
            return work(tx)
        return self._db.run_transaction(work, writer=MESHJOIN_WRITER)


def _write_rows(txn: Database, rows: Sequence[EnrichedRow]) -> None:
    """Dimensions first, because each foreign key needs its parent row to exist.

    Rows are never de-duplicated. Exact duplicates in the source stay separate facts.
    """
    if not rows:
        return
    customers = _resolve_keys(txn, _CUSTOMER, rows)
    products = _resolve_keys(txn, _PRODUCT, rows)
    suppliers = _resolve_keys(txn, _SUPPLIER, rows)
    stores = _resolve_keys(txn, _STORE, rows)
    times = _resolve_keys(txn, _TIME, rows)
    txn.executemany(
        _INSERT_FACT,
        [
            (
                customers[row.customer_id],
                products[row.product_id],
                suppliers[row.supplier_id],
                stores[row.store_id],
                times[row.time["time_key"]],
                row.transaction_id,
                row.sales_mode,
                row.processing_mode,
                row.quantity,
                row.revenue,
            )
            for row in rows
        ],
    )
