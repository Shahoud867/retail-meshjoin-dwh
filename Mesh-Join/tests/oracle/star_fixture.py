"""A warehouse loaded straight from the raw file, with no MESHJOIN in the path.

It has two uses. The OLAP queries can be proved correct before the pipeline exists, and
later a pipeline-loaded warehouse can be compared with this one row by row. The second
use is why the rules come from independent_rules: a comparison between two copies of the
same code would prove nothing.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Hashable, Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

from retail_dw.config import ConnectionConfig
from retail_dw.db import Database, ensure_database, locate_create_dw_script, run_sql_script
from tests.oracle.independent_rules import enriched_rows

DEFAULT_DATABASE = "t_olap_oracle"
EXPECTED_FACT_ROWS = 397_884
_EXPECTED_REVENUE = Decimal("8911407.904")
_EXPECTED_DIMENSION_ROWS = {
    "dim_customer": 4_338,
    "dim_product": 3_665,
    "dim_supplier": 5,
    "dim_store": 5,
    "dim_time": 17_282,
}

_INSERT_BATCH_ROWS = 5_000
_EXAMPLES_PER_TABLE = 5

_CUSTOMER_COLUMNS = (
    "customer_id", "gender", "age_group", "occupation", "city_category",
    "stay_in_current_city_years", "marital_status",
)
_PRODUCT_COLUMNS = ("product_id", "product_name", "product_category")
_SUPPLIER_COLUMNS = ("supplier_id", "supplier_name")
_STORE_COLUMNS = ("store_id", "store_name", "city")
_TIME_COLUMNS = (
    "time_key", "full_datetime", "full_date", "hour_of_day", "day_of_month", "month_no",
    "month_name", "quarter_no", "year_no", "day_of_week", "day_name", "day_type", "season",
    "half_year",
)

# Natural keys only. Surrogate keys differ between warehouses by design, loaded_at is a
# clock reading, and processing_mode legitimately varies (a periodic row may be Shed),
# so that one is checked by a rule of its own in _lineage_violations.
_COMPARED_QUERIES = {
    "dim_customer": f"SELECT {', '.join(_CUSTOMER_COLUMNS)} FROM dim_customer",
    "dim_product": f"SELECT {', '.join(_PRODUCT_COLUMNS)} FROM dim_product",
    "dim_supplier": f"SELECT {', '.join(_SUPPLIER_COLUMNS)} FROM dim_supplier",
    "dim_store": f"SELECT {', '.join(_STORE_COLUMNS)} FROM dim_store",
    "dim_time": f"SELECT {', '.join(_TIME_COLUMNS)} FROM dim_time",
    "fact_sales": (
        "SELECT f.transaction_id, c.customer_id, p.product_id, su.supplier_id, st.store_id,"
        " f.time_key, f.sales_mode, f.quantity, f.revenue"
        " FROM fact_sales AS f"
        " JOIN dim_customer AS c ON c.customer_key = f.customer_key"
        " JOIN dim_product AS p ON p.product_key = f.product_key"
        " JOIN dim_supplier AS su ON su.supplier_key = f.supplier_key"
        " JOIN dim_store AS st ON st.store_key = f.store_key"
    ),
}
_ALLOWED_LINEAGE = frozenset(
    {("Online", "Live"), ("Periodic", "Periodic"), ("Periodic", "Shed")}
)


def _insert_sql(table: str, columns: Sequence[str]) -> str:
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"


def _hash_order(frame: pd.DataFrame, natural_column: str, key_salt: str) -> pd.DataFrame:
    """Insert dimension rows in an order unrelated to their ids, so surrogate keys carry
    no meaning here either. The pipeline assigns keys in arrival order, and a query that
    secretly relied on key order would then fail against this warehouse too."""
    digests = [
        hashlib.md5(f"{key_salt}{value}".encode()).digest() for value in frame[natural_column]
    ]
    return frame.iloc[sorted(range(len(digests)), key=digests.__getitem__)]


def _plain_rows(frame: pd.DataFrame, columns: Sequence[str]) -> list[tuple]:
    """Rows as Python values, because the MySQL driver refuses numpy scalars."""
    return list(frame[list(columns)].astype(object).itertuples(index=False, name=None))


def _load_dimension(
    db: Database,
    frame: pd.DataFrame,
    table: str,
    columns: Sequence[str],
    surrogate: str,
    key_salt: str,
) -> dict[Hashable, int]:
    """Insert the distinct rows of one dimension and return natural id -> surrogate key."""
    natural = columns[0]
    distinct = _hash_order(frame.drop_duplicates(natural), natural, key_salt)
    db.executemany(_insert_sql(table, columns), _plain_rows(distinct, columns), _INSERT_BATCH_ROWS)
    return dict(db.query(f"SELECT {natural}, {surrogate} FROM {table}"))


def _load_time_dimension(db: Database, frame: pd.DataFrame, key_salt: str) -> None:
    distinct = frame.drop_duplicates("time_key").assign(
        full_datetime=lambda rows: rows["event_time"]
    )
    rows = [
        (*row[:1], row[1].to_pydatetime(), *row[2:])
        for row in _plain_rows(_hash_order(distinct, "time_key", key_salt), _TIME_COLUMNS)
    ]
    db.executemany(_insert_sql("dim_time", _TIME_COLUMNS), rows, _INSERT_BATCH_ROWS)


def load_star(cfg: ConnectionConfig, frame: pd.DataFrame, key_salt: str = "") -> None:
    """Recreate the schema in cfg's database and load the given rows into the star. A
    different key_salt gives the same content with different surrogate keys."""
    ensure_database(cfg)
    with Database(cfg) as db:
        run_sql_script(db, locate_create_dw_script())
        customer_keys = _load_dimension(
            db, frame, "dim_customer", _CUSTOMER_COLUMNS, "customer_key", key_salt
        )
        product_keys = _load_dimension(
            db, frame, "dim_product", _PRODUCT_COLUMNS, "product_key", key_salt
        )
        supplier_keys = _load_dimension(
            db, frame, "dim_supplier", _SUPPLIER_COLUMNS, "supplier_key", key_salt
        )
        store_keys = _load_dimension(
            db, frame, "dim_store", _STORE_COLUMNS, "store_key", key_salt
        )
        _load_time_dimension(db, frame, key_salt)

        fact_columns = (
            "customer_key", "product_key", "supplier_key", "store_key", "time_key",
            "transaction_id", "sales_mode", "processing_mode", "quantity", "revenue",
        )
        rows = (
            (
                customer_keys[customer],
                product_keys[product],
                supplier_keys[supplier],
                store_keys[store],
                time_key,
                transaction,
                sales_mode,
                processing_mode,
                quantity,
                Decimal(mills).scaleb(-3),
            )
            for customer, product, supplier, store, time_key, transaction, sales_mode,
            processing_mode, quantity, mills in _plain_rows(
                frame,
                (
                    "customer_id", "product_id", "supplier_id", "store_id", "time_key",
                    "transaction_id", "sales_mode", "processing_mode", "quantity",
                    "revenue_mills",
                ),
            )
        )
        db.executemany(_insert_sql("fact_sales", fact_columns), rows, _INSERT_BATCH_ROWS)


def _is_complete(db: Database) -> bool:
    """A cheap test that the database holds the whole file, not an interrupted load.
    Row counts catch a load that stopped early and the revenue total catches old data."""
    tables = {
        name
        for (name,) in db.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
        )
    }
    if not {"fact_sales", *_EXPECTED_DIMENSION_ROWS} <= tables:
        return False
    for table, expected in _EXPECTED_DIMENSION_ROWS.items():
        if db.query(f"SELECT COUNT(*) FROM {table}")[0][0] != expected:
            return False
    ((rows, revenue),) = db.query("SELECT COUNT(*), SUM(revenue) FROM fact_sales")
    return rows == EXPECTED_FACT_ROWS and revenue == _EXPECTED_REVENUE


def ensure_star(cfg: ConnectionConfig) -> None:
    """Load the full file into cfg's database unless a complete copy is already there.
    Reading and loading take a few minutes, so this database is kept between runs."""
    ensure_database(cfg)
    with Database(cfg) as db:
        if _is_complete(db):
            return
    load_star(cfg, enriched_rows())
    with Database(cfg) as db:
        if not _is_complete(db):
            raise RuntimeError(f"Loading {cfg.database} did not produce the expected star")


@dataclass(frozen=True)
class TableDifference:
    """Rows of one table found in only one of the warehouses, counting duplicates."""

    table: str
    only_in_reference: int
    only_in_candidate: int
    examples: tuple[tuple, ...]


@dataclass(frozen=True)
class WarehouseComparison:
    """The result of comparing two warehouses. Use `identical` and, on failure, `describe`."""

    rows_compared: dict[str, tuple[int, int]]
    differences: tuple[TableDifference, ...]
    lineage_violations: tuple[tuple[str, str, int], ...]

    @property
    def identical(self) -> bool:
        return not self.differences and not self.lineage_violations

    def describe(self) -> str:
        """A readable summary for a test failure message."""
        lines = [
            f"{table}: reference {left:,} rows, candidate {right:,} rows"
            for table, (left, right) in self.rows_compared.items()
        ]
        for difference in self.differences:
            lines.append(
                f"{difference.table}: {difference.only_in_reference:,} only in reference, "
                f"{difference.only_in_candidate:,} only in candidate"
            )
            lines.extend(f"    e.g. {example}" for example in difference.examples)
        lines.extend(
            f"bad lineage: sales_mode={sales}, processing_mode={processing}, rows={count:,}"
            for sales, processing, count in self.lineage_violations
        )
        return "\n".join(lines)


def _difference(table: str, reference: Counter, candidate: Counter) -> TableDifference | None:
    """Compare as multisets. The source has exact duplicate rows that must all survive, so
    a set comparison would let the pipeline lose or double one without being noticed."""
    only_reference = reference - candidate
    only_candidate = candidate - reference
    if not only_reference and not only_candidate:
        return None
    return TableDifference(
        table=table,
        only_in_reference=only_reference.total(),
        only_in_candidate=only_candidate.total(),
        examples=_first_examples(only_reference, only_candidate),
    )


def _first_examples(*counters: Iterable[tuple]) -> tuple[tuple, ...]:
    examples: list[tuple] = []
    for counter in counters:
        for row in counter:
            if len(examples) == _EXAMPLES_PER_TABLE:
                return tuple(examples)
            examples.append(row)
    return tuple(examples)


def _lineage_violations(db: Database) -> tuple[tuple[str, str, int], ...]:
    """processing_mode may vary, but only within what the sales_mode of the row allows."""
    groups = db.query(
        "SELECT sales_mode, processing_mode, COUNT(*) FROM fact_sales"
        " GROUP BY sales_mode, processing_mode"
    )
    return tuple(group for group in groups if (group[0], group[1]) not in _ALLOWED_LINEAGE)


def compare_warehouses(
    reference: ConnectionConfig, candidate: ConnectionConfig
) -> WarehouseComparison:
    """Compare two warehouses row by row on natural keys, dimensions and fact.

    Keys and load times are expected to differ. A full match means every fact row has the
    same customer, product, supplier, store, minute, cart, sales mode, quantity and exact
    revenue, the same number of times, and every dimension row has the same attributes.
    """
    rows_compared: dict[str, tuple[int, int]] = {}
    differences: list[TableDifference] = []
    with Database(reference) as left, Database(candidate) as right:
        for table, sql in _COMPARED_QUERIES.items():
            left_rows = Counter(left.query(sql))
            right_rows = Counter(right.query(sql))
            rows_compared[table] = (left_rows.total(), right_rows.total())
            difference = _difference(table, left_rows, right_rows)
            if difference is not None:
                differences.append(difference)
        violations = _lineage_violations(right)
    return WarehouseComparison(rows_compared, tuple(differences), violations)
