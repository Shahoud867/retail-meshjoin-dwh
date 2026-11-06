"""The bonus cache against a real MySQL server, including failures in the middle of a
flush: a row must end up in the cache or in the fact table, never both, never neither."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.db import LOST_DURING_QUERY_ERROR, Database, MySqlError
from retail_dw.load.consolidate import EnrichedRow, consolidate
from retail_dw.load.live_cache import LiveCache
from retail_dw.load.loader import MESHJOIN_WRITER, Loader
from retail_dw.meshjoin.model import (
    PROCESSING_LIVE,
    PROCESSING_PERIODIC,
    PROCESSING_SHED,
    SALES_MODE_ONLINE,
    SALES_MODE_PERIODIC,
    Transaction,
)

pytestmark = pytest.mark.integration

WHEN = datetime(2011, 3, 5, 14, 30)
DIMENSION_TABLES = ("dim_customer", "dim_product", "dim_supplier", "dim_store", "dim_time")


def _row(
    number: int = 0,
    processing_mode: str = PROCESSING_LIVE,
    sales_mode: str = SALES_MODE_ONLINE,
    quantity: int = 2,
    unit_price: str = "1.25",
) -> EnrichedRow:
    customer_id = str(13000 + number % 7)
    product_id = f"P{number % 5}"
    tx = Transaction(
        str(540000 + number),
        customer_id,
        product_id,
        quantity,
        Decimal(unit_price),
        WHEN + timedelta(minutes=number % 11),
        sales_mode,
        processing_mode,
    )
    tx.customer = (customer_id, "Male", "18-25", "Clerk", "C", "1", "Single")
    tx.product = (product_id, f"NAME {product_id}", "Kitchen", "SUP02", "Atlantic Gifts Ltd")
    return consolidate(tx)


def _count(db: Database, table: str) -> int:
    return db.query(f"SELECT COUNT(*) FROM {table}")[0][0]


def _txn_seq(db: Database) -> int:
    rows = db.query("SELECT txn_seq FROM etl_progress WHERE writer = %s", (MESHJOIN_WRITER,))
    return rows[0][0] if rows else 0


def _fill(db: Database, cache: LiveCache, rows: Sequence[EnrichedRow]) -> None:
    db.run_transaction(lambda txn: cache.add(rows, txn), writer=MESHJOIN_WRITER)
    cache.record_added(len(rows))


def _cached_transactions(db: Database) -> list[str]:
    return [r[0] for r in db.query("SELECT transaction_id FROM live_cache ORDER BY cache_id")]


def _fact_transactions(db: Database) -> list[str]:
    return [r[0] for r in db.query("SELECT transaction_id FROM fact_sales ORDER BY sales_key")]


def _kill(cfg: ConnectionConfig, connection_id: int) -> None:
    with Database(cfg) as admin:
        admin.execute(f"KILL {int(connection_id)}")


class _FaultyLoader(Loader):
    """Runs the real load and then acts, with the fact rows sent but not committed."""

    def __init__(self, db: Database, after_load: Any) -> None:
        super().__init__(db)
        self._after_load = after_load

    def load(self, rows: Any, extra_in_txn: Any = None, tx: Database | None = None) -> int:
        loaded = super().load(rows, extra_in_txn, tx)
        self._after_load(tx)
        return loaded


class _FailingDelete(Database):
    """Refuses the cache DELETE, the last statement of a flush."""

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        if sql.startswith("DELETE FROM live_cache"):
            raise RuntimeError("injected at delete")
        return super().execute(sql, params)


class TestAdd:
    def test_rows_are_stored_enriched_and_in_order(self, clean_db: Database) -> None:
        cache = LiveCache(clean_db)
        rows = [_row(i) for i in range(5)]
        _fill(clean_db, cache, rows)
        stored = clean_db.query(
            "SELECT transaction_id, customer_id, gender, product_id, product_name, "
            "supplier_id, store_id, quantity, revenue, sales_mode, processing_mode "
            "FROM live_cache ORDER BY cache_id"
        )
        assert stored == [
            (
                r.transaction_id, r.customer_id, "Male", r.product_id, r.product_name,
                "SUP02", 1, 2, Decimal("2.500"), "Online", "Live",
            )
            for r in rows
        ]

    def test_add_writes_nothing_to_the_warehouse(self, clean_db: Database) -> None:
        _fill(clean_db, LiveCache(clean_db), [_row()])
        for table in (*DIMENSION_TABLES, "fact_sales"):
            assert _count(clean_db, table) == 0

    @pytest.mark.parametrize("mode", [PROCESSING_PERIODIC, PROCESSING_SHED])
    def test_any_other_mode_is_refused_and_the_whole_batch_with_it(
        self, clean_db: Database, mode: str
    ) -> None:
        cache = LiveCache(clean_db)
        batch = [_row(1), _row(2, processing_mode=mode, sales_mode=SALES_MODE_PERIODIC)]
        with pytest.raises(ValueError, match="Only Live"):
            clean_db.run_transaction(lambda txn: cache.add(batch, txn), writer=MESHJOIN_WRITER)
        assert _count(clean_db, "live_cache") == 0
        assert cache.count() == 0

    def test_empty_add_writes_nothing(self, clean_db: Database) -> None:
        with clean_db.transaction() as txn:
            assert LiveCache(clean_db).add([], txn) == 0
        assert _count(clean_db, "live_cache") == 0


class TestCount:
    def test_count_starts_from_the_table_so_a_resume_is_right(
        self, clean_db: Database
    ) -> None:
        first = LiveCache(clean_db)
        _fill(clean_db, first, [_row(i) for i in range(4)])
        assert LiveCache(clean_db).count() == 4

    def test_count_follows_adds_and_flushes(self, clean_db: Database) -> None:
        cache = LiveCache(clean_db)
        loader = Loader(clean_db)
        _fill(clean_db, cache, [_row(i) for i in range(10)])
        assert cache.count() == 10
        cache.flush_slice(loader, limit=3)
        assert cache.count() == 7
        _fill(clean_db, cache, [_row(i) for i in range(10, 12)])
        assert cache.count() == 9
        cache.flush_slice(loader, limit=100)
        assert cache.count() == 0
        assert _count(clean_db, "live_cache") == 0

    def test_a_rolled_back_add_never_counts(self, clean_db: Database) -> None:
        cache = LiveCache(clean_db)

        def add_then_fail(txn: Database) -> None:
            cache.add([_row()], txn)
            raise RuntimeError("injected")

        with pytest.raises(RuntimeError):
            clean_db.run_transaction(add_then_fail, writer=MESHJOIN_WRITER)
        assert cache.count() == 0
        assert _count(clean_db, "live_cache") == 0


class TestFlushSlice:
    def test_moves_the_oldest_rows_in_order_and_leaves_the_rest(
        self, clean_db: Database
    ) -> None:
        cache = LiveCache(clean_db)
        rows = [_row(i) for i in range(10)]
        _fill(clean_db, cache, rows)
        kept_ids = clean_db.query("SELECT cache_id FROM live_cache ORDER BY cache_id")[4:]

        assert cache.flush_slice(Loader(clean_db), limit=4) == 4

        assert _fact_transactions(clean_db) == [r.transaction_id for r in rows[:4]]
        assert _cached_transactions(clean_db) == [r.transaction_id for r in rows[4:]]
        assert clean_db.query("SELECT cache_id FROM live_cache ORDER BY cache_id") == kept_ids

    def test_repeated_flushes_load_everything_once_in_file_order(
        self, clean_db: Database
    ) -> None:
        cache = LiveCache(clean_db)
        loader = Loader(clean_db)
        rows = [_row(i) for i in range(25)]
        _fill(clean_db, cache, rows)
        moved = []
        while cache.count():
            moved.append(cache.flush_slice(loader, limit=10))
        assert moved == [10, 10, 5]
        assert _fact_transactions(clean_db) == [r.transaction_id for r in rows]

    def test_flush_inserts_the_dimensions_it_needs(self, clean_db: Database) -> None:
        cache = LiveCache(clean_db)
        rows = [_row(i) for i in range(30)]
        _fill(clean_db, cache, rows)
        cache.flush_slice(Loader(clean_db))
        assert _count(clean_db, "dim_customer") == 7
        assert _count(clean_db, "dim_product") == 5
        assert _count(clean_db, "dim_supplier") == 1
        assert _count(clean_db, "dim_store") == 1
        assert _count(clean_db, "dim_time") == 11

    def test_flushed_row_equals_a_direct_load(self, clean_db: Database) -> None:
        """Whichever way a Live row travels, the warehouse must end up the same."""
        rows = [_row(1, quantity=80995, unit_price="2.08")]
        cache = LiveCache(clean_db)
        _fill(clean_db, cache, rows)
        cache.flush_slice(Loader(clean_db))
        read_fact = (
            "SELECT c.customer_id, c.gender, p.product_id, p.product_name, u.supplier_id, "
            "s.store_id, s.store_name, s.city, t.time_key, f.transaction_id, f.sales_mode, "
            "f.processing_mode, f.quantity, f.revenue FROM fact_sales f "
            "JOIN dim_customer c ON c.customer_key = f.customer_key "
            "JOIN dim_product p ON p.product_key = f.product_key "
            "JOIN dim_supplier u ON u.supplier_key = f.supplier_key "
            "JOIN dim_store s ON s.store_key = f.store_key "
            "JOIN dim_time t ON t.time_key = f.time_key"
        )
        through_cache = clean_db.query(read_fact)
        for table in ("fact_sales", *DIMENSION_TABLES):
            clean_db.execute(f"DELETE FROM {table}")
        Loader(clean_db).load(rows)
        assert clean_db.query(read_fact) == through_cache
        assert str(through_cache[0][-1]) == "168469.600"

    def test_revenue_comes_from_the_cache_and_is_not_computed_again(
        self, clean_db: Database
    ) -> None:
        cache = LiveCache(clean_db)
        _fill(clean_db, cache, [_row()])
        clean_db.execute("UPDATE live_cache SET revenue = 42.125")
        cache.flush_slice(Loader(clean_db))
        assert clean_db.query("SELECT revenue FROM fact_sales") == [(Decimal("42.125"),)]

    def test_exact_duplicates_stay_separate_through_the_cache(
        self, clean_db: Database
    ) -> None:
        cache = LiveCache(clean_db)
        _fill(clean_db, cache, [_row(3), _row(3)])
        assert cache.flush_slice(Loader(clean_db)) == 2
        assert _fact_transactions(clean_db) == [_row(3).transaction_id] * 2

    def test_empty_cache_does_nothing_at_all(self, clean_db: Database) -> None:
        cache = LiveCache(clean_db)
        before = _txn_seq(clean_db)
        assert cache.flush_slice(Loader(clean_db)) == 0
        assert _txn_seq(clean_db) == before, "an idle loop must not write"

    def test_flush_is_one_transaction(self, clean_db: Database) -> None:
        cache = LiveCache(clean_db)
        _fill(clean_db, cache, [_row(i) for i in range(5)])
        before = _txn_seq(clean_db)
        cache.flush_slice(Loader(clean_db))
        assert _txn_seq(clean_db) == before + 1

    def test_row_added_during_a_flush_is_not_deleted(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        """A flush deletes what it copied and nothing else, so a row that arrives
        meanwhile stays in the cache and waits for the next flush."""
        cache = LiveCache(clean_db)
        _fill(clean_db, cache, [_row(i) for i in range(3)])

        def add_from_another_session(_txn: Database | None) -> None:
            with Database(temp_database) as other:
                other_cache = LiveCache(other)
                other.run_transaction(lambda txn: other_cache.add([_row(99)], txn))

        moved = cache.flush_slice(_FaultyLoader(clean_db, add_from_another_session))
        assert moved == 3
        assert _cached_transactions(clean_db) == [_row(99).transaction_id]
        assert _count(clean_db, "fact_sales") == 3


class TestFlushAtomicity:
    """After any failure: nothing moved, nothing lost, and the count still right."""

    ROWS = 12

    def _filled_cache(self, db: Database) -> tuple[LiveCache, list[EnrichedRow]]:
        cache = LiveCache(db)
        rows = [_row(i) for i in range(self.ROWS)]
        _fill(db, cache, rows)
        return cache, rows

    @staticmethod
    def _assert_nothing_moved(db: Database, cache: LiveCache, rows: list[EnrichedRow]) -> None:
        assert _cached_transactions(db) == [r.transaction_id for r in rows]
        assert cache.count() == len(rows)
        for table in (*DIMENSION_TABLES, "fact_sales"):
            assert _count(db, table) == 0, f"{table} should have rolled back"

    def _assert_moved_once(
        self, db: Database, cache: LiveCache, rows: list[EnrichedRow], moved: int
    ) -> None:
        assert moved == self.ROWS
        assert _fact_transactions(db) == [r.transaction_id for r in rows]
        assert _count(db, "live_cache") == 0
        assert cache.count() == 0

    def test_failure_after_the_fact_insert_moves_nothing(self, clean_db: Database) -> None:
        cache, rows = self._filled_cache(clean_db)

        def fail(_txn: Database | None) -> None:
            raise RuntimeError("injected after load")

        with pytest.raises(RuntimeError, match="injected after load"):
            cache.flush_slice(_FaultyLoader(clean_db, fail))
        self._assert_nothing_moved(clean_db, cache, rows)

    def test_failure_at_the_delete_moves_nothing(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        cache, rows = self._filled_cache(clean_db)
        with _FailingDelete(temp_database) as session:
            flushing = LiveCache(session)
            with pytest.raises(RuntimeError, match="injected at delete"):
                flushing.flush_slice(Loader(session))
            assert flushing.count() == self.ROWS
        self._assert_nothing_moved(clean_db, cache, rows)

    def test_flush_after_a_failure_completes_with_no_duplicates(
        self, clean_db: Database
    ) -> None:
        cache, rows = self._filled_cache(clean_db)

        def fail(_txn: Database | None) -> None:
            raise RuntimeError("injected")

        with pytest.raises(RuntimeError):
            cache.flush_slice(_FaultyLoader(clean_db, fail))
        moved = cache.flush_slice(Loader(clean_db))
        self._assert_moved_once(clean_db, cache, rows, moved)

    def test_kill_in_the_middle_of_a_flush_retries_with_no_duplicates(
        self, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        cache, rows = self._filled_cache(clean_db)
        runs: list[int] = []

        def kill_on_first_run(txn: Database | None) -> None:
            assert txn is not None
            runs.append(txn.connection_id)
            if len(runs) == 1:
                _kill(db_cfg, txn.connection_id)

        moved = cache.flush_slice(_FaultyLoader(clean_db, kill_on_first_run))
        assert len(runs) == 2
        self._assert_moved_once(clean_db, cache, rows, moved)

    def test_kill_at_commit_runs_again_and_counts_once(
        self, clean_db: Database, temp_database: ConnectionConfig, db_cfg: ConnectionConfig
    ) -> None:
        cache, rows = self._filled_cache(clean_db)
        with Database(temp_database) as session:
            first_session = session.connection_id
            flushing = LiveCache(session)
            real_commit = session._conn.commit

            def commit() -> None:
                _kill(db_cfg, session.connection_id)
                real_commit()

            session._conn.commit = commit  # type: ignore[method-assign]
            moved = flushing.flush_slice(Loader(session))
            assert session.connection_id != first_session
            assert flushing.count() == 0
        assert moved == self.ROWS
        assert _fact_transactions(clean_db) == [r.transaction_id for r in rows]
        assert _count(clean_db, "live_cache") == 0

    def test_commit_that_landed_with_its_reply_lost_counts_once(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        cache, rows = self._filled_cache(clean_db)
        with Database(temp_database) as session:
            flushing = LiveCache(session)
            real_commit = session._conn.commit

            def commit() -> None:
                real_commit()
                raise MySqlError(msg="simulated lost reply", errno=LOST_DURING_QUERY_ERROR)

            session._conn.commit = commit  # type: ignore[method-assign]
            moved = flushing.flush_slice(Loader(session))
            assert flushing.count() == 0
        assert moved == self.ROWS
        assert _fact_transactions(clean_db) == [r.transaction_id for r in rows]
        assert _count(clean_db, "live_cache") == 0
