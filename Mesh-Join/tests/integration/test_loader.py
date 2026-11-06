"""The Loader against a real MySQL server. Connection losses are real KILLs, except a
COMMIT whose reply is lost, which needs the commit to land first."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.db import LOST_DURING_QUERY_ERROR, Database, MySqlError
from retail_dw.load.consolidate import EnrichedRow, consolidate
from retail_dw.load.loader import MESHJOIN_WRITER, Loader
from retail_dw.meshjoin.model import (
    PROCESSING_LIVE,
    PROCESSING_PERIODIC,
    PROCESSING_SHED,
    SALES_MODE_ONLINE,
    SALES_MODE_PERIODIC,
    Transaction,
)
from retail_dw.prepare.rules import time_parts

pytestmark = pytest.mark.integration

ER_CHECK_VIOLATED = 3819
WHEN = datetime(2010, 12, 1, 8, 26)
DIMENSION_TABLES = ("dim_customer", "dim_product", "dim_supplier", "dim_store", "dim_time")


def _row(
    transaction_id: str = "536365",
    customer_id: str = "17850",
    product_id: str = "85123A",
    quantity: int = 6,
    unit_price: str = "2.55",
    when: datetime = WHEN,
    sales_mode: str = SALES_MODE_ONLINE,
    processing_mode: str = PROCESSING_LIVE,
    product_name: str = "WHITE HANGING HEART",
    supplier: tuple[str, str] = ("SUP01", "Northwind Supplies"),
) -> EnrichedRow:
    tx = Transaction(
        transaction_id,
        customer_id,
        product_id,
        quantity,
        Decimal(unit_price),
        when,
        sales_mode,
        processing_mode,
    )
    tx.customer = (customer_id, "Female", "26-35", "Engineer", "B", "4+", "Married")
    tx.product = (product_id, product_name, "Home Decor", *supplier)
    return consolidate(tx)


def _rows_of_a_big_batch(size: int) -> list[EnrichedRow]:
    return [
        _row(
            transaction_id=str(536365 + i % 400),
            customer_id=str(12000 + i % 1200),
            product_id=f"P{i % 700}",
            quantity=i % 9 + 1,
            unit_price="1.25",
            when=WHEN + timedelta(minutes=i % 1000),
            supplier=(f"SUP0{i % 5 + 1}", f"Supplier {i % 5 + 1}"),
        )
        for i in range(size)
    ]


def _count(db: Database, table: str) -> int:
    return db.query(f"SELECT COUNT(*) FROM {table}")[0][0]


def _txn_seq(db: Database) -> int:
    rows = db.query("SELECT txn_seq FROM etl_progress WHERE writer = %s", (MESHJOIN_WRITER,))
    return rows[0][0] if rows else 0


def _kill(cfg: ConnectionConfig, connection_id: int) -> None:
    with Database(cfg) as admin:
        admin.execute(f"KILL {int(connection_id)}")


class _WatchedDatabase(Database):
    """A real session that records its statements and can act just before one runs."""

    def __init__(self, cfg: ConnectionConfig) -> None:
        self.statements: list[str] = []
        self.before: Callable[[str], None] | None = None
        super().__init__(cfg)

    def _note(self, sql: str) -> None:
        self.statements.append(sql)
        if self.before is not None:
            self.before(sql)

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
        self._note(sql)
        return super().query(sql, params)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        self._note(sql)
        return super().execute(sql, params)

    def executemany(
        self, sql: str, rows: Iterable[Sequence[Any]], batch_size: int = 1000
    ) -> int:
        self._note(sql)
        return super().executemany(sql, rows, batch_size)


class _KilledAfterFirstFactBatch(Database):
    """Dies between two batches of the fact insert, with rows already sent."""

    def __init__(self, cfg: ConnectionConfig, admin_cfg: ConnectionConfig) -> None:
        self._admin_cfg = admin_cfg
        self.killed = False
        super().__init__(cfg)

    def executemany(
        self, sql: str, rows: Iterable[Sequence[Any]], batch_size: int = 1000
    ) -> int:
        batch = list(rows)
        if self.killed or "fact_sales" not in sql:
            return super().executemany(sql, batch, batch_size)
        self.killed = True
        super().executemany(sql, batch[:batch_size], batch_size)
        _kill(self._admin_cfg, self.connection_id)
        return super().executemany(sql, batch[batch_size:], batch_size)


def _kill_once_before(
    session: _WatchedDatabase, admin_cfg: ConnectionConfig, fragment: str
) -> list[str]:
    """Arrange one KILL just before the first statement containing fragment."""
    fired: list[str] = []

    def before(sql: str) -> None:
        if fragment in sql and not fired:
            fired.append(sql)
            _kill(admin_cfg, session.connection_id)

    session.before = before
    return fired


def _lose_next_commit_reply(session: Database) -> None:
    """The COMMIT lands on the server and the client then sees the connection drop.
    A reconnect replaces the connection object, so this fires once."""
    real_commit = session._conn.commit

    def commit() -> None:
        real_commit()
        raise MySqlError(msg="simulated lost reply", errno=LOST_DURING_QUERY_ERROR)

    session._conn.commit = commit  # type: ignore[method-assign]


@pytest.fixture
def watched(clean_db: Database, temp_database: ConnectionConfig) -> Any:
    with _WatchedDatabase(temp_database) as session:
        yield session


class TestDimensionsThenFact:
    def test_one_row_reaches_the_fact_with_the_keys_of_its_own_dimensions(
        self, clean_db: Database
    ) -> None:
        loaded = Loader(clean_db).load([_row()])
        assert loaded == 1
        rows = clean_db.query(
            """
            SELECT c.customer_id, c.gender, c.age_group, c.occupation, c.city_category,
                   c.stay_in_current_city_years, c.marital_status,
                   p.product_id, p.product_name, p.product_category,
                   u.supplier_id, u.supplier_name,
                   s.store_id, s.store_name, s.city,
                   t.time_key, t.full_datetime,
                   f.transaction_id, f.sales_mode, f.processing_mode, f.quantity, f.revenue
            FROM fact_sales f
            JOIN dim_customer c ON c.customer_key = f.customer_key
            JOIN dim_product p ON p.product_key = f.product_key
            JOIN dim_supplier u ON u.supplier_key = f.supplier_key
            JOIN dim_store s ON s.store_key = f.store_key
            JOIN dim_time t ON t.time_key = f.time_key
            """
        )
        assert rows == [
            (
                "17850", "Female", "26-35", "Engineer", "B", "4+", "Married",
                "85123A", "WHITE HANGING HEART", "Home Decor",
                "SUP01", "Northwind Supplies",
                1, "Online", "Web",
                201012010826, WHEN,
                "536365", "Online", "Live", 6, Decimal("15.300"),
            )
        ]

    def test_every_dimension_insert_comes_before_the_fact_insert(
        self, watched: _WatchedDatabase
    ) -> None:
        Loader(watched).load([_row()])
        inserts = [
            sql.split()[2] for sql in watched.statements if sql.startswith("INSERT INTO")
        ]
        inserts = [table for table in inserts if table != "etl_progress"]
        assert sorted(inserts[:-1]) == sorted(DIMENSION_TABLES)
        assert inserts[-1] == "fact_sales"

    def test_time_dimension_uses_its_deterministic_key_and_all_parts(
        self, clean_db: Database
    ) -> None:
        Loader(clean_db).load([_row()])
        columns = [
            "time_key", "full_datetime", "full_date", "hour_of_day", "day_of_month",
            "month_no", "month_name", "quarter_no", "year_no", "day_of_week",
            "day_name", "day_type", "season", "half_year",
        ]
        stored = clean_db.query(f"SELECT {', '.join(columns)} FROM dim_time")
        parts = time_parts(WHEN)
        assert stored == [tuple(parts[column] for column in columns)]
        assert stored[0][0] == 201012010826

    def test_rows_of_one_minute_share_one_time_row(self, clean_db: Database) -> None:
        Loader(clean_db).load([_row(product_id="A"), _row(product_id="B")])
        assert _count(clean_db, "dim_time") == 1


class TestDimensionDeduplication:
    def test_same_values_in_later_batches_add_no_dimension_rows(
        self, clean_db: Database
    ) -> None:
        loader = Loader(clean_db)
        loader.load([_row()])
        before = {t: clean_db.query(f"SELECT * FROM {t}") for t in DIMENSION_TABLES}
        for _ in range(3):
            loader.load([_row()])
        assert {t: clean_db.query(f"SELECT * FROM {t}") for t in DIMENSION_TABLES} == before
        assert _count(clean_db, "fact_sales") == 4

    def test_only_the_missing_value_is_inserted(self, watched: _WatchedDatabase) -> None:
        loader = Loader(watched)
        loader.load([_row()])
        watched.statements.clear()
        loader.load([_row(), _row(product_id="NEW1")])
        inserted = [sql.split()[2] for sql in watched.statements if sql.startswith("INSERT INTO")]
        assert inserted == ["etl_progress", "dim_product", "fact_sales"]

    def test_a_batch_of_existing_values_inserts_only_facts(
        self, watched: _WatchedDatabase
    ) -> None:
        loader = Loader(watched)
        loader.load([_row()])
        watched.statements.clear()
        loader.load([_row(), _row()])
        inserted = [sql.split()[2] for sql in watched.statements if sql.startswith("INSERT INTO")]
        assert inserted == ["etl_progress", "fact_sales"]

    def test_all_stores_and_suppliers_come_through_their_natural_ids(
        self, clean_db: Database
    ) -> None:
        rows = [
            _row(transaction_id=str(536000 + i), sales_mode=SALES_MODE_PERIODIC,
                 processing_mode=PROCESSING_PERIODIC, supplier=(f"SUP0{i % 5 + 1}", f"S{i % 5 + 1}"))
            for i in range(60)
        ]
        Loader(clean_db).load(rows)
        stored = clean_db.query(
            "SELECT s.store_id, u.supplier_id FROM fact_sales f "
            "JOIN dim_store s ON s.store_key = f.store_key "
            "JOIN dim_supplier u ON u.supplier_key = f.supplier_key "
            "ORDER BY f.sales_key"
        )
        assert stored == [(row.store_id, row.supplier_id) for row in rows]


class TestSurrogateKeysAreNotBurned:
    @staticmethod
    def _keys(db: Database, table: str, key: str) -> list[int]:
        return [row[0] for row in db.query(f"SELECT {key} FROM {table} ORDER BY {key}")]

    def test_repeated_values_use_up_no_keys(self, schema_db: Database) -> None:
        """Freshly created tables, so the counters start at 1. Any gap is a burned key."""
        loader = Loader(schema_db)
        for batch in range(6):
            loader.load(
                [
                    _row(
                        customer_id=str(17000 + i),
                        product_id=f"P{i}",
                        supplier=(f"SUP0{i % 5 + 1}", f"S{i % 5 + 1}"),
                    )
                    for i in range(batch * 3 + 1)
                ]
            )
        assert self._keys(schema_db, "dim_customer", "customer_key") == list(range(1, 17))
        assert self._keys(schema_db, "dim_product", "product_key") == list(range(1, 17))
        assert self._keys(schema_db, "dim_supplier", "supplier_key") == list(range(1, 6))
        assert self._keys(schema_db, "dim_store", "store_key") == [1]

    def test_next_new_value_gets_the_next_key(self, schema_db: Database) -> None:
        loader = Loader(schema_db)
        loader.load([_row(customer_id="1")])
        for _ in range(5):
            loader.load([_row(customer_id="1")])
        loader.load([_row(customer_id="2")])
        assert self._keys(schema_db, "dim_customer", "customer_key") == [1, 2]


class TestFactGrain:
    def test_exact_duplicates_in_one_batch_stay_separate_facts(
        self, clean_db: Database
    ) -> None:
        """Same cart, same product, same minute: the real file has 5,194 of these."""
        Loader(clean_db).load([_row(), _row(), _row(product_id="OTHER")])
        rows = clean_db.query(
            "SELECT p.product_id, f.transaction_id, f.time_key, f.quantity, f.revenue "
            "FROM fact_sales f JOIN dim_product p ON p.product_key = f.product_key "
            "ORDER BY f.sales_key"
        )
        duplicate = ("85123A", "536365", 201012010826, 6, Decimal("15.300"))
        assert rows == [
            duplicate,
            duplicate,
            ("OTHER", "536365", 201012010826, 6, Decimal("15.300")),
        ]
        assert _count(clean_db, "dim_product") == 2

    def test_exact_duplicates_across_batches_stay_separate_facts(
        self, clean_db: Database
    ) -> None:
        loader = Loader(clean_db)
        loader.load([_row()])
        loader.load([_row()])
        assert _count(clean_db, "fact_sales") == 2

    def test_product_ids_that_differ_only_by_case_stay_separate(
        self, clean_db: Database
    ) -> None:
        loader = Loader(clean_db)
        loader.load([_row(product_id="85123A", product_name="UPPER")])
        loader.load(
            [
                _row(product_id="85123a", product_name="LOWER"),
                _row(product_id="85123A", product_name="UPPER"),
            ]
        )
        rows = clean_db.query(
            "SELECT p.product_id, p.product_name FROM fact_sales f "
            "JOIN dim_product p ON p.product_key = f.product_key ORDER BY f.sales_key"
        )
        assert rows == [("85123A", "UPPER"), ("85123a", "LOWER"), ("85123A", "UPPER")]
        assert _count(clean_db, "dim_product") == 2

    def test_rows_keep_their_batch_order(self, clean_db: Database) -> None:
        rows = [_row(transaction_id=str(536400 - i)) for i in range(20)]
        Loader(clean_db).load(rows)
        stored = clean_db.query("SELECT transaction_id FROM fact_sales ORDER BY sales_key")
        assert [row[0] for row in stored] == [row.transaction_id for row in rows]


class TestExactValues:
    @pytest.mark.parametrize(
        ("quantity", "price", "expected"),
        [(80995, "2.08", "168469.600"), (1, "0.001", "0.001"), (74215, "1.04", "77183.600")],
    )
    def test_decimals_round_trip_exactly(
        self, clean_db: Database, quantity: int, price: str, expected: str
    ) -> None:
        Loader(clean_db).load([_row(quantity=quantity, unit_price=price)])
        assert clean_db.query("SELECT quantity, revenue FROM fact_sales") == [
            (quantity, Decimal(expected))
        ]
        assert str(clean_db.query("SELECT revenue FROM fact_sales")[0][0]) == expected

    def test_sum_of_many_small_revenues_is_exact(self, clean_db: Database) -> None:
        Loader(clean_db).load([_row(quantity=1, unit_price="0.001") for _ in range(1000)])
        assert clean_db.query("SELECT SUM(revenue) FROM fact_sales") == [(Decimal("1.000"),)]

    @pytest.mark.parametrize(
        ("sales_mode", "processing_mode"),
        [
            (SALES_MODE_ONLINE, PROCESSING_LIVE),
            (SALES_MODE_PERIODIC, PROCESSING_PERIODIC),
            (SALES_MODE_PERIODIC, PROCESSING_SHED),
        ],
    )
    def test_modes_are_stored_unchanged(
        self, clean_db: Database, sales_mode: str, processing_mode: str
    ) -> None:
        Loader(clean_db).load([_row(sales_mode=sales_mode, processing_mode=processing_mode)])
        assert clean_db.query("SELECT sales_mode, processing_mode FROM fact_sales") == [
            (sales_mode, processing_mode)
        ]

    def test_awkward_text_arrives_unchanged(self, clean_db: Database) -> None:
        name = "50% OFF BOY'S \"RED\" \\ %s CAFÉ"
        Loader(clean_db).load([_row(product_name=name)])
        assert clean_db.query("SELECT product_name FROM dim_product") == [(name,)]


class TestOneTransaction:
    def test_each_load_raises_txn_seq_exactly_once(self, clean_db: Database) -> None:
        loader = Loader(clean_db)
        loader.load([_row()])
        assert _txn_seq(clean_db) == 1
        loader.load([_row(), _row()])
        assert _txn_seq(clean_db) == 2

    def test_failed_fact_insert_rolls_the_dimensions_back(self, clean_db: Database) -> None:
        with pytest.raises(MySqlError) as caught:
            Loader(clean_db).load([_row(), _row(customer_id="99", quantity=0)])
        assert caught.value.errno == ER_CHECK_VIOLATED
        for table in (*DIMENSION_TABLES, "fact_sales"):
            assert _count(clean_db, table) == 0
        assert _txn_seq(clean_db) == 0

    def test_extra_work_commits_with_the_rows(self, clean_db: Database) -> None:
        Loader(clean_db).load(
            [_row()],
            extra_in_txn=lambda txn: txn.execute(
                "UPDATE etl_progress SET sim_hour = 7 WHERE writer = %s", (MESHJOIN_WRITER,)
            ),
        )
        assert _count(clean_db, "fact_sales") == 1
        assert clean_db.query("SELECT sim_hour FROM etl_progress WHERE writer = 'meshjoin'") == [
            (7,)
        ]

    def test_extra_work_that_fails_rolls_the_rows_back(self, clean_db: Database) -> None:
        def fail(_txn: Database) -> None:
            raise RuntimeError("injected")

        with pytest.raises(RuntimeError, match="injected"):
            Loader(clean_db).load([_row()], extra_in_txn=fail)
        for table in (*DIMENSION_TABLES, "fact_sales"):
            assert _count(clean_db, table) == 0

    def test_joining_a_transaction_leaves_the_commit_to_its_owner(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        loader = Loader(clean_db)
        with Database(temp_database) as observer:
            with clean_db.transaction("meshjoin") as txn:
                assert loader.load([_row()], tx=txn) == 1
                assert _count(observer, "fact_sales") == 0, "must not commit by itself"
            assert _count(observer, "fact_sales") == 1
        assert _txn_seq(clean_db) == 1, "the owner's transaction is the only one"

    def test_joined_load_rolls_back_with_its_owner(self, clean_db: Database) -> None:
        with pytest.raises(RuntimeError, match="owner failed"):
            with clean_db.transaction("meshjoin") as txn:
                Loader(clean_db).load([_row()], tx=txn)
                raise RuntimeError("owner failed")
        assert _count(clean_db, "fact_sales") == 0
        assert _count(clean_db, "dim_customer") == 0

    def test_empty_batch_writes_nothing(self, clean_db: Database) -> None:
        assert Loader(clean_db).load([]) == 0
        assert _txn_seq(clean_db) == 0


class TestConnectionLoss:
    """Whatever the moment of the loss, the batch must end up in the warehouse once."""

    SIZE = 2500

    def _assert_loaded_exactly_once(self, db: Database, rows: list[EnrichedRow]) -> None:
        stored = db.query(
            "SELECT transaction_id, quantity, revenue FROM fact_sales ORDER BY sales_key"
        )
        assert stored == [(r.transaction_id, r.quantity, r.revenue) for r in rows]
        assert _count(db, "dim_customer") == len({r.customer_id for r in rows})
        assert _count(db, "dim_product") == len({r.product_id for r in rows})
        assert _count(db, "dim_supplier") == len({r.supplier_id for r in rows})
        assert _count(db, "dim_store") == len({r.store_id for r in rows})
        assert _count(db, "dim_time") == len({r.time["time_key"] for r in rows})

    @pytest.mark.parametrize(
        "statement",
        ["etl_progress", "SELECT customer_id", "INTO dim_customer", "INTO dim_time", "INTO fact_sales"],
    )
    def test_kill_before_a_statement_retries_cleanly(
        self,
        watched: _WatchedDatabase,
        clean_db: Database,
        db_cfg: ConnectionConfig,
        statement: str,
    ) -> None:
        rows = _rows_of_a_big_batch(self.SIZE)
        first_session = watched.connection_id
        fired = _kill_once_before(watched, db_cfg, statement)

        assert Loader(watched).load(rows) == self.SIZE

        assert fired, "the kill must really have happened"
        assert watched.connection_id != first_session
        self._assert_loaded_exactly_once(clean_db, rows)
        assert _txn_seq(clean_db) == 1, "the killed attempt must not count"

    def test_kill_between_fact_batches_retries_cleanly(
        self, clean_db: Database, temp_database: ConnectionConfig, db_cfg: ConnectionConfig
    ) -> None:
        rows = _rows_of_a_big_batch(self.SIZE)
        with _KilledAfterFirstFactBatch(temp_database, db_cfg) as session:
            assert Loader(session).load(rows) == self.SIZE
            assert session.killed
        self._assert_loaded_exactly_once(clean_db, rows)

    def test_kill_just_before_commit_retries_cleanly(
        self, clean_db: Database, temp_database: ConnectionConfig, db_cfg: ConnectionConfig
    ) -> None:
        rows = _rows_of_a_big_batch(self.SIZE)
        runs: list[int] = []

        def kill_on_first_run(txn: Database) -> None:
            runs.append(txn.connection_id)
            if len(runs) == 1:
                _kill(db_cfg, txn.connection_id)

        with Database(temp_database) as session:
            Loader(session).load(rows, extra_in_txn=kill_on_first_run)
        assert len(runs) == 2
        self._assert_loaded_exactly_once(clean_db, rows)

    def test_commit_that_landed_with_its_reply_lost_is_not_run_again(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        rows = _rows_of_a_big_batch(self.SIZE)
        runs: list[int] = []
        with Database(temp_database) as session:
            first_session = session.connection_id
            _lose_next_commit_reply(session)
            loaded = Loader(session).load(rows, extra_in_txn=lambda txn: runs.append(1))
            assert session.connection_id != first_session
        assert loaded == self.SIZE
        assert len(runs) == 1, "the commit landed, so the work must not run again"
        self._assert_loaded_exactly_once(clean_db, rows)

    def test_a_generator_of_rows_survives_the_retry(
        self, watched: _WatchedDatabase, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        rows = _rows_of_a_big_batch(20)
        _kill_once_before(watched, db_cfg, "INTO fact_sales")
        assert Loader(watched).load(row for row in rows) == 20
        self._assert_loaded_exactly_once(clean_db, rows)


class TestScale:
    @staticmethod
    def _questions(db: Database) -> int:
        return int(db.query("SHOW SESSION STATUS LIKE 'Questions'")[0][1])

    @pytest.mark.parametrize(
        ("size", "statement_limit"),
        [(500, 25), (5000, 50)],
    )
    def test_batch_uses_a_small_fixed_number_of_statements(
        self, clean_db: Database, size: int, statement_limit: int
    ) -> None:
        """Counted by the server. A chunk of new values takes 20 statements plus the
        one that reads this counter. One statement per row would be thousands."""
        rows = _rows_of_a_big_batch(size)
        loader = Loader(clean_db)
        before = self._questions(clean_db)
        loader.load(rows)
        used = self._questions(clean_db) - before
        assert used <= statement_limit
        assert clean_db.query("SELECT COUNT(*), SUM(quantity), SUM(revenue) FROM fact_sales") == [
            (
                size,
                Decimal(sum(r.quantity for r in rows)),
                sum(r.revenue for r in rows),
            )
        ]

    def test_second_batch_of_known_values_needs_no_dimension_writes(
        self, watched: _WatchedDatabase
    ) -> None:
        rows = _rows_of_a_big_batch(5000)
        loader = Loader(watched)
        loader.load(rows)
        watched.statements.clear()
        loader.load(rows)
        assert not [sql for sql in watched.statements if sql.startswith("INSERT INTO dim_")]

    def test_no_key_survives_from_one_batch_to_the_next(self, clean_db: Database) -> None:
        """A long-lived map would still hold the old keys and break the foreign keys."""
        loader = Loader(clean_db)
        loader.load([_row()])
        old_key = clean_db.query("SELECT customer_key FROM dim_customer")[0][0]
        for table in ("fact_sales", *DIMENSION_TABLES):
            clean_db.execute(f"DELETE FROM {table}")
        loader.load([_row()])
        new_key = clean_db.query("SELECT customer_key FROM dim_customer")[0][0]
        assert new_key != old_key
        assert clean_db.query("SELECT customer_key FROM fact_sales") == [(new_key,)]
