"""ResultSink against a real MySQL server: routing, exact resume offset, staging flags,
rejects and metrics. Memory must only change after the database has committed."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from retail_dw.config import LIVE_LOAD_CACHE, LIVE_LOAD_DIRECT, ConnectionConfig
from retail_dw.db import (
    LOST_DURING_QUERY_ERROR,
    CommitOutcomeUnknownError,
    Database,
    MySqlError,
)
from retail_dw.load import sink as sink_module
from retail_dw.load.live_cache import LiveCache
from retail_dw.load.loader import MESHJOIN_WRITER, Loader
from retail_dw.load.sink import (
    REASON_NO_CUSTOMER,
    REASON_NO_MATCH,
    REASON_NO_PRODUCT,
    STAGING_LOADED,
    STAGING_REJECTED,
    ResultSink,
    StagingMismatchError,
)
from retail_dw.meshjoin.model import (
    PROCESSING_LIVE,
    PROCESSING_PERIODIC,
    PROCESSING_SHED,
    SALES_MODE_ONLINE,
    SALES_MODE_PERIODIC,
    Chunk,
    Transaction,
)
from retail_dw.metrics import (
    LIVE_COMMITTED,
    LIVE_LOADED,
    PERIODIC_LOADED,
    REJECTED,
    SHED_LOADED,
    Metrics,
)

pytestmark = pytest.mark.integration

WHEN = datetime(2011, 6, 15, 10, 0)
DIMENSION_TABLES = ("dim_customer", "dim_product", "dim_supplier", "dim_store", "dim_time")
BOTH_MODES = [LIVE_LOAD_CACHE, LIVE_LOAD_DIRECT]


def _enrich(tx: Transaction, customer: bool, product: bool) -> Transaction:
    if customer:
        tx.customer = (tx.customer_id, "Female", "36-45", "Teacher", "A", "3", "Single")
    if product:
        tx.product = (tx.product_id, f"NAME {tx.product_id}", "Toys", "SUP04", "Celtic Craft Co")
    return tx


def _live(
    number: int, customer: bool = True, product: bool = True, enqueued_at: float = 0.0
) -> Transaction:
    tx = Transaction(
        str(550000 + number),
        str(14000 + number % 5),
        f"L{number % 4}",
        2,
        Decimal("1.50"),
        WHEN + timedelta(minutes=number % 3),
        SALES_MODE_ONLINE,
        PROCESSING_LIVE,
        enqueued_at=enqueued_at,
    )
    return _enrich(tx, customer, product)


def _staged(
    db: Database,
    number: int,
    mode: str = PROCESSING_PERIODIC,
    customer: bool = True,
    product: bool = True,
) -> Transaction:
    """A periodic transaction with its real staging row, as if already released."""
    transaction_id = str(560000 + number)
    customer_id = str(15000 + number % 5)
    product_id = f"S{number % 4}"
    event_time = WHEN + timedelta(minutes=number % 3)
    db.execute(
        "INSERT INTO periodic_sales_data (transaction_id, customer_id, product_id, "
        "quantity, unit_price, event_time, sales_mode, release_hour, released) "
        "VALUES (%s, %s, %s, 3, 2.25, %s, 'Periodic', 0, 1)",
        (transaction_id, customer_id, product_id, event_time),
    )
    staging_id = db.query("SELECT LAST_INSERT_ID()")[0][0]
    tx = Transaction(
        transaction_id,
        customer_id,
        product_id,
        3,
        Decimal("2.250"),
        event_time,
        SALES_MODE_PERIODIC,
        mode,
        staging_id=staging_id,
    )
    return _enrich(tx, customer, product)


def _chunk(*transactions: Transaction) -> Chunk:
    return Chunk(1, list(transactions))


def _count(db: Database, table: str) -> int:
    return db.query(f"SELECT COUNT(*) FROM {table}")[0][0]


def _online_committed(db: Database) -> int:
    rows = db.query(
        "SELECT online_rows_committed FROM etl_progress WHERE writer = %s", (MESHJOIN_WRITER,)
    )
    return rows[0][0] if rows else 0


def _txn_seq(db: Database) -> int:
    rows = db.query("SELECT txn_seq FROM etl_progress WHERE writer = %s", (MESHJOIN_WRITER,))
    return rows[0][0] if rows else 0


def _fact_transactions(db: Database) -> list[str]:
    return [r[0] for r in db.query("SELECT transaction_id FROM fact_sales ORDER BY sales_key")]


def _cache_transactions(db: Database) -> list[str]:
    return [r[0] for r in db.query("SELECT transaction_id FROM live_cache ORDER BY cache_id")]


def _staging_flags(db: Database) -> dict[int, int]:
    return dict(db.query("SELECT staging_id, loaded FROM periodic_sales_data"))


def _kill(cfg: ConnectionConfig, connection_id: int) -> None:
    with Database(cfg) as admin:
        admin.execute(f"KILL {int(connection_id)}")


def _sink(db: Database, live_load: str = LIVE_LOAD_CACHE) -> tuple[ResultSink, LiveCache, Metrics]:
    cache = LiveCache(db)
    metrics = Metrics()
    return ResultSink(db, Loader(db), cache, live_load, metrics), cache, metrics


class _Faulty(Database):
    """A real session with a chosen action just before a chosen statement. The action
    fires once, after skipping the first `skip` matches. Used both to KILL the session
    and to raise an ordinary error."""

    def __init__(self, cfg: ConnectionConfig) -> None:
        self._fragment: str | None = None
        self._action: Any = None
        self._skip = 0
        self.fired = False
        super().__init__(cfg)

    def arm(self, fragment: str, action: Any, skip: int = 0) -> None:
        self._fragment = fragment
        self._action = action
        self._skip = skip

    def _check(self, sql: str) -> None:
        if self._fragment is None or self._fragment not in sql or self.fired:
            return
        if self._skip:
            self._skip -= 1
            return
        self.fired = True
        self._action()

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
        self._check(sql)
        return super().query(sql, params)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        self._check(sql)
        return super().execute(sql, params)

    def executemany(
        self, sql: str, rows: Iterable[Sequence[Any]], batch_size: int = 1000
    ) -> int:
        self._check(sql)
        return super().executemany(sql, rows, batch_size)


def _raise_injected() -> None:
    raise RuntimeError("injected")


def _lose_next_commit_reply(session: Database) -> None:
    real_commit = session._conn.commit

    def commit() -> None:
        real_commit()
        raise MySqlError(msg="simulated lost reply", errno=LOST_DURING_QUERY_ERROR)

    session._conn.commit = commit  # type: ignore[method-assign]


def _kill_at_next_commit(session: Database, admin_cfg: ConnectionConfig) -> None:
    """The session dies with all the work done and COMMIT about to be sent, so the work
    runs a second time on a new connection. Counting it twice is the risk here."""
    real_commit = session._conn.commit

    def commit() -> None:
        _kill(admin_cfg, session.connection_id)
        real_commit()

    session._conn.commit = commit  # type: ignore[method-assign]


@pytest.fixture
def faulty(clean_db: Database, temp_database: ConnectionConfig) -> Any:
    with _Faulty(temp_database) as session:
        yield session


class TestLiveRouting:
    def test_cache_mode_writes_the_cache_and_not_the_warehouse(
        self, clean_db: Database
    ) -> None:
        sink, cache, _ = _sink(clean_db, LIVE_LOAD_CACHE)
        sink.handle([_chunk(_live(0), _live(1))])
        assert _cache_transactions(clean_db) == ["550000", "550001"]
        assert cache.count() == 2
        for table in (*DIMENSION_TABLES, "fact_sales"):
            assert _count(clean_db, table) == 0

    def test_direct_mode_writes_the_warehouse_and_not_the_cache(
        self, clean_db: Database
    ) -> None:
        sink, cache, _ = _sink(clean_db, LIVE_LOAD_DIRECT)
        sink.handle([_chunk(_live(0), _live(1))])
        assert _fact_transactions(clean_db) == ["550000", "550001"]
        assert _count(clean_db, "live_cache") == 0
        assert cache.count() == 0

    def test_live_rows_are_not_held(self, clean_db: Database) -> None:
        sink, _, _ = _sink(clean_db)
        sink.handle([_chunk(_live(0))])
        assert sink.held_count == 0

    @pytest.mark.parametrize("live_load", BOTH_MODES)
    def test_rows_of_all_chunks_commit_in_file_order(
        self, clean_db: Database, live_load: str
    ) -> None:
        sink, _, _ = _sink(clean_db, live_load)
        numbers = list(range(12))
        sink.handle(
            [
                _chunk(*[_live(i) for i in numbers[:5]]),
                _chunk(*[_live(i) for i in numbers[5:9]]),
                _chunk(*[_live(i) for i in numbers[9:]]),
            ]
        )
        read = _cache_transactions if live_load == LIVE_LOAD_CACHE else _fact_transactions
        assert read(clean_db) == [str(550000 + i) for i in numbers]

    def test_each_handle_is_one_transaction(self, clean_db: Database) -> None:
        sink, _, _ = _sink(clean_db, LIVE_LOAD_DIRECT)
        sink.handle([_chunk(_live(0)), _chunk(_live(1))])
        assert _txn_seq(clean_db) == 1
        sink.handle([_chunk(_live(2))])
        assert _txn_seq(clean_db) == 2

    def test_nothing_to_commit_means_no_transaction(self, clean_db: Database) -> None:
        sink, _, _ = _sink(clean_db)
        sink.handle([])
        sink.handle([_chunk()])
        sink.handle([_chunk(_staged(clean_db, 0))])
        assert _txn_seq(clean_db) == 0

    def test_unknown_live_load_is_refused(self, clean_db: Database) -> None:
        with pytest.raises(ValueError, match="live load"):
            ResultSink(clean_db, Loader(clean_db), LiveCache(clean_db), "fast", Metrics())

    def test_flush_cache_slice_moves_rows_and_counts_them_as_loaded(
        self, clean_db: Database
    ) -> None:
        sink, cache, metrics = _sink(clean_db, LIVE_LOAD_CACHE)
        sink.handle([_chunk(*[_live(i) for i in range(5)])])
        assert metrics.total(LIVE_LOADED) == 0
        assert sink.flush_cache_slice(limit=3) == 3
        assert metrics.total(LIVE_LOADED) == 3
        assert cache.count() == 2
        assert _fact_transactions(clean_db) == ["550000", "550001", "550002"]

    def test_direct_mode_still_empties_a_cache_left_by_an_earlier_run(
        self, clean_db: Database
    ) -> None:
        earlier, _, _ = _sink(clean_db, LIVE_LOAD_CACHE)
        earlier.handle([_chunk(_live(0))])
        resumed, cache, _ = _sink(clean_db, LIVE_LOAD_DIRECT)
        assert cache.count() == 1
        assert resumed.flush_cache_slice() == 1
        assert cache.count() == 0
        assert _fact_transactions(clean_db) == ["550000"]


class TestOnlineRowsCommitted:
    @pytest.mark.parametrize("live_load", BOTH_MODES)
    def test_exactly_right_across_chunk_batches(
        self, clean_db: Database, live_load: str
    ) -> None:
        """A mix of Live, unmatched Live and periodic rows. Only Live rows count, and a
        rejected Live row counts because it must not be read again on resume."""
        sink, _, _ = _sink(clean_db, live_load)
        expected = 0
        number = 0
        for chunk_count, live_in_chunk in [(1, 7), (3, 11), (2, 1), (4, 500), (1, 13)]:
            chunks = []
            for _ in range(chunk_count):
                transactions = [_staged(clean_db, number)]
                for _ in range(live_in_chunk):
                    transactions.append(_live(number, customer=number % 6 != 0))
                    number += 1
                    expected += 1
                chunks.append(_chunk(*transactions))
            sink.handle(chunks)
            assert _online_committed(clean_db) == expected

    def test_rejected_live_rows_advance_it(self, clean_db: Database) -> None:
        sink, _, _ = _sink(clean_db)
        sink.handle([_chunk(_live(0, customer=False), _live(1, product=False))])
        assert _online_committed(clean_db) == 2
        assert _count(clean_db, "etl_reject") == 2
        assert _count(clean_db, "live_cache") == 0

    def test_periodic_and_shed_rows_do_not_advance_it(self, clean_db: Database) -> None:
        sink, _, _ = _sink(clean_db)
        sink.handle(
            [
                _chunk(
                    _staged(clean_db, 0),
                    _staged(clean_db, 1, PROCESSING_SHED),
                    _staged(clean_db, 2, product=False),
                )
            ]
        )
        assert _online_committed(clean_db) == 0

    @pytest.mark.parametrize("live_load", BOTH_MODES)
    def test_rows_and_progress_commit_together_or_not_at_all(
        self, faulty: _Faulty, clean_db: Database, live_load: str
    ) -> None:
        sink, cache, metrics = _sink(faulty, live_load)
        faulty.arm("online_rows_committed", _raise_injected)
        with pytest.raises(RuntimeError, match="injected"):
            sink.handle([_chunk(_live(0), _live(1, customer=False))])
        assert _online_committed(clean_db) == 0
        assert _count(clean_db, "live_cache") == 0
        assert _count(clean_db, "fact_sales") == 0
        assert _count(clean_db, "dim_customer") == 0
        assert _count(clean_db, "etl_reject") == 0
        assert cache.count() == 0
        assert metrics.total(LIVE_COMMITTED) == 0
        assert metrics.total(REJECTED) == 0

    @pytest.mark.parametrize(
        ("live_load", "statement"),
        [
            (LIVE_LOAD_CACHE, "etl_progress"),
            (LIVE_LOAD_CACHE, "INTO live_cache"),
            (LIVE_LOAD_CACHE, "INTO etl_reject"),
            (LIVE_LOAD_CACHE, "online_rows_committed"),
            (LIVE_LOAD_DIRECT, "etl_progress"),
            (LIVE_LOAD_DIRECT, "INTO dim_customer"),
            (LIVE_LOAD_DIRECT, "INTO fact_sales"),
            (LIVE_LOAD_DIRECT, "INTO etl_reject"),
            (LIVE_LOAD_DIRECT, "online_rows_committed"),
        ],
    )
    def test_kill_at_any_statement_keeps_rows_and_progress_exact(
        self,
        faulty: _Faulty,
        clean_db: Database,
        db_cfg: ConnectionConfig,
        live_load: str,
        statement: str,
    ) -> None:
        sink, cache, metrics = _sink(faulty, live_load)
        faulty.arm(statement, lambda: _kill(db_cfg, faulty.connection_id))

        sink.handle([_chunk(*[_live(i) for i in range(6)], _live(6, product=False))])

        assert faulty.fired
        read = _cache_transactions if live_load == LIVE_LOAD_CACHE else _fact_transactions
        assert read(clean_db) == [str(550000 + i) for i in range(6)]
        assert _count(clean_db, "etl_reject") == 1
        assert _online_committed(clean_db) == 7
        assert _txn_seq(clean_db) == 1
        assert cache.count() == (6 if live_load == LIVE_LOAD_CACHE else 0)
        assert metrics.total(LIVE_COMMITTED) == 6
        assert metrics.total(REJECTED) == 1

    @pytest.mark.parametrize("live_load", BOTH_MODES)
    def test_commit_that_landed_with_its_reply_lost_counts_once(
        self, clean_db: Database, temp_database: ConnectionConfig, live_load: str
    ) -> None:
        with Database(temp_database) as session:
            sink, cache, metrics = _sink(session, live_load)
            _lose_next_commit_reply(session)
            sink.handle([_chunk(*[_live(i) for i in range(4)])])
            assert cache.count() == (4 if live_load == LIVE_LOAD_CACHE else 0)
            assert metrics.total(LIVE_COMMITTED) == 4
        read = _cache_transactions if live_load == LIVE_LOAD_CACHE else _fact_transactions
        assert read(clean_db) == [str(550000 + i) for i in range(4)]
        assert _online_committed(clean_db) == 4
        assert _txn_seq(clean_db) == 1

    @pytest.mark.parametrize("live_load", BOTH_MODES)
    def test_kill_at_commit_runs_again_and_counts_once(
        self,
        clean_db: Database,
        temp_database: ConnectionConfig,
        db_cfg: ConnectionConfig,
        live_load: str,
    ) -> None:
        with Database(temp_database) as session:
            first_session = session.connection_id
            sink, cache, metrics = _sink(session, live_load)
            _kill_at_next_commit(session, db_cfg)
            sink.handle([_chunk(*[_live(i) for i in range(4)], _live(4, customer=False))])
            assert session.connection_id != first_session
            assert cache.count() == (4 if live_load == LIVE_LOAD_CACHE else 0)
            assert metrics.total(LIVE_COMMITTED) == 4
            assert metrics.total(REJECTED) == 1
            assert metrics.total(LIVE_LOADED) == (0 if live_load == LIVE_LOAD_CACHE else 4)
        read = _cache_transactions if live_load == LIVE_LOAD_CACHE else _fact_transactions
        assert read(clean_db) == [str(550000 + i) for i in range(4)]
        assert _count(clean_db, "etl_reject") == 1
        assert _online_committed(clean_db) == 5
        assert _txn_seq(clean_db) == 1

    def test_failed_commit_holds_no_rows_from_that_call(self, faulty: _Faulty) -> None:
        sink, _, _ = _sink(faulty)
        faulty.arm("online_rows_committed", _raise_injected)
        with pytest.raises(RuntimeError, match="injected"):
            sink.handle([_chunk(_live(0), _staged(faulty, 0))])
        assert sink.held_count == 0

    def test_unknown_commit_outcome_reaches_the_caller_and_changes_no_memory(
        self,
        clean_db: Database,
        temp_database: ConnectionConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The run must stop and direct the operator to --resume, not guess."""

        def outcome_unknown(_writer: str) -> bool:
            raise CommitOutcomeUnknownError("lost COMMIT is still holding its lock")

        with Database(temp_database) as session:
            sink, cache, metrics = _sink(session)
            monkeypatch.setattr(session, "_commit_landed", outcome_unknown)
            _lose_next_commit_reply(session)
            with pytest.raises(CommitOutcomeUnknownError):
                sink.handle([_chunk(_live(0))])
            assert cache.count() == 0
            assert metrics.total(LIVE_COMMITTED) == 0


class TestHeldRows:
    @pytest.mark.parametrize("live_load", BOTH_MODES)
    @pytest.mark.parametrize("mode", [PROCESSING_PERIODIC, PROCESSING_SHED])
    def test_periodic_and_shed_rows_are_held_and_never_reach_the_cache(
        self, clean_db: Database, live_load: str, mode: str
    ) -> None:
        sink, cache, _ = _sink(clean_db, live_load)
        sink.handle([_chunk(_staged(clean_db, 0, mode), _staged(clean_db, 1, mode))])
        assert sink.held_count == 2
        assert _count(clean_db, "live_cache") == 0
        assert cache.count() == 0
        assert _count(clean_db, "fact_sales") == 0
        assert set(_staging_flags(clean_db).values()) == {0}
        sink.flush_held()
        assert _count(clean_db, "live_cache") == 0
        assert _count(clean_db, "fact_sales") == 2

    def test_flush_loads_rows_with_their_lineage_and_sets_loaded(
        self, clean_db: Database
    ) -> None:
        sink, _, metrics = _sink(clean_db)
        sink.handle(
            [
                _chunk(
                    _staged(clean_db, 0, PROCESSING_PERIODIC),
                    _staged(clean_db, 1, PROCESSING_SHED),
                    _staged(clean_db, 2, PROCESSING_PERIODIC),
                )
            ]
        )
        assert sink.flush_held() == 3
        assert sink.held_count == 0
        assert clean_db.query(
            "SELECT transaction_id, sales_mode, processing_mode, quantity, revenue "
            "FROM fact_sales ORDER BY sales_key"
        ) == [
            ("560000", "Periodic", "Periodic", 3, Decimal("6.750")),
            ("560001", "Periodic", "Shed", 3, Decimal("6.750")),
            ("560002", "Periodic", "Periodic", 3, Decimal("6.750")),
        ]
        assert set(_staging_flags(clean_db).values()) == {STAGING_LOADED}
        assert metrics.total(PERIODIC_LOADED) == 2
        assert metrics.total(SHED_LOADED) == 1
        assert metrics.total(LIVE_COMMITTED) == 0

    def test_flush_with_nothing_held_writes_nothing(self, clean_db: Database) -> None:
        sink, _, _ = _sink(clean_db)
        assert sink.flush_held() == 0
        assert _txn_seq(clean_db) == 0

    def test_each_slice_is_one_transaction(
        self, clean_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_module, "PERIODIC_LOAD_SLICE", 3)
        sink, _, _ = _sink(clean_db)
        sink.handle([_chunk(*[_staged(clean_db, i) for i in range(7)])])
        assert sink.flush_held() == 7
        assert _txn_seq(clean_db) == 3, "7 rows in slices of 3 is 3 transactions"
        assert _fact_transactions(clean_db) == [str(560000 + i) for i in range(7)]

    def test_real_slice_size_loads_more_than_one_slice(self, clean_db: Database) -> None:
        sink, _, metrics = _sink(clean_db)
        transactions = [_staged(clean_db, i) for i in range(5003)]
        sink.handle([_chunk(*transactions)])
        assert sink.held_count == 5003
        assert sink.flush_held() == 5003
        assert _txn_seq(clean_db) == 2
        assert _count(clean_db, "fact_sales") == 5003
        assert set(_staging_flags(clean_db).values()) == {STAGING_LOADED}
        assert metrics.total(PERIODIC_LOADED) == 5003

    def test_failed_slice_leaves_loaded_flags_only_on_rows_that_loaded(
        self, faulty: _Faulty, clean_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_module, "PERIODIC_LOAD_SLICE", 2)
        transactions = [_staged(clean_db, i) for i in range(5)]
        sink, _, metrics = _sink(faulty)
        sink.handle([_chunk(*transactions)])

        faulty.arm("UPDATE periodic_sales_data", _raise_injected, skip=1)
        with pytest.raises(RuntimeError, match="injected"):
            sink.flush_held()

        flags = _staging_flags(clean_db)
        ids = [tx.staging_id for tx in transactions]
        assert [flags[i] for i in ids] == [1, 1, 0, 0, 0]
        assert _fact_transactions(clean_db) == ["560000", "560001"]
        assert sink.held_count == 3
        assert metrics.total(PERIODIC_LOADED) == 2

        assert sink.flush_held() == 3
        assert _fact_transactions(clean_db) == [str(560000 + i) for i in range(5)]
        assert set(_staging_flags(clean_db).values()) == {STAGING_LOADED}
        assert metrics.total(PERIODIC_LOADED) == 5

    def test_failure_at_the_flag_update_loads_nothing(
        self, faulty: _Faulty, clean_db: Database
    ) -> None:
        sink, _, metrics = _sink(faulty)
        sink.handle([_chunk(_staged(clean_db, 0), _staged(clean_db, 1))])
        faulty.arm("UPDATE periodic_sales_data", _raise_injected)
        with pytest.raises(RuntimeError, match="injected"):
            sink.flush_held()
        assert _count(clean_db, "fact_sales") == 0
        assert _count(clean_db, "dim_customer") == 0
        assert set(_staging_flags(clean_db).values()) == {0}
        assert sink.held_count == 2
        assert metrics.total(PERIODIC_LOADED) == 0

    @pytest.mark.parametrize(
        "statement", ["INTO fact_sales", "UPDATE periodic_sales_data", "INTO dim_product"]
    )
    def test_kill_during_a_flush_keeps_rows_and_flags_exact(
        self, faulty: _Faulty, clean_db: Database, db_cfg: ConnectionConfig, statement: str
    ) -> None:
        sink, _, metrics = _sink(faulty)
        sink.handle([_chunk(*[_staged(clean_db, i) for i in range(6)])])
        faulty.arm(statement, lambda: _kill(db_cfg, faulty.connection_id))
        assert sink.flush_held() == 6
        assert faulty.fired
        assert _fact_transactions(clean_db) == [str(560000 + i) for i in range(6)]
        assert set(_staging_flags(clean_db).values()) == {STAGING_LOADED}
        assert sink.held_count == 0
        assert metrics.total(PERIODIC_LOADED) == 6

    def test_kill_at_commit_runs_the_slice_again_and_counts_once(
        self, clean_db: Database, temp_database: ConnectionConfig, db_cfg: ConnectionConfig
    ) -> None:
        with Database(temp_database) as session:
            sink, _, metrics = _sink(session)
            sink.handle([_chunk(*[_staged(clean_db, i) for i in range(4)])])
            _kill_at_next_commit(session, db_cfg)
            assert sink.flush_held() == 4
            assert sink.held_count == 0
            assert metrics.total(PERIODIC_LOADED) == 4
        assert _fact_transactions(clean_db) == [str(560000 + i) for i in range(4)]
        assert set(_staging_flags(clean_db).values()) == {STAGING_LOADED}

    def test_commit_that_landed_with_its_reply_lost_is_not_loaded_twice(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        with Database(temp_database) as session:
            sink, _, metrics = _sink(session)
            sink.handle([_chunk(*[_staged(clean_db, i) for i in range(4)])])
            _lose_next_commit_reply(session)
            assert sink.flush_held() == 4
            assert sink.held_count == 0
            assert metrics.total(PERIODIC_LOADED) == 4
        assert _count(clean_db, "fact_sales") == 4
        assert set(_staging_flags(clean_db).values()) == {STAGING_LOADED}

    def test_a_row_with_no_staging_row_stops_the_load(self, clean_db: Database) -> None:
        """Without its flag such a row would be loaded a second time after --resume."""
        sink, _, _ = _sink(clean_db)
        orphan = _staged(clean_db, 0)
        clean_db.execute("DELETE FROM periodic_sales_data")
        sink.handle([_chunk(orphan)])
        with pytest.raises(StagingMismatchError):
            sink.flush_held()
        assert _count(clean_db, "fact_sales") == 0
        assert sink.held_count == 1

    def test_a_row_already_marked_loaded_is_not_loaded_again(
        self, clean_db: Database
    ) -> None:
        sink, _, _ = _sink(clean_db)
        tx = _staged(clean_db, 0)
        clean_db.execute(
            "UPDATE periodic_sales_data SET loaded = 1 WHERE staging_id = %s", (tx.staging_id,)
        )
        sink.handle([_chunk(tx)])
        with pytest.raises(StagingMismatchError):
            sink.flush_held()
        assert _count(clean_db, "fact_sales") == 0


class TestRejects:
    @pytest.mark.parametrize(
        ("customer", "product", "reason"),
        [
            (False, True, REASON_NO_CUSTOMER),
            (True, False, REASON_NO_PRODUCT),
            (False, False, REASON_NO_MATCH),
        ],
    )
    @pytest.mark.parametrize("live_load", BOTH_MODES)
    def test_unmatched_live_row_is_rejected_with_its_reason_and_never_partly_loaded(
        self,
        clean_db: Database,
        live_load: str,
        customer: bool,
        product: bool,
        reason: str,
    ) -> None:
        sink, cache, metrics = _sink(clean_db, live_load)
        sink.handle([_chunk(_live(1, customer=customer, product=product))])
        assert clean_db.query(
            "SELECT transaction_id, customer_id, product_id, quantity, unit_price, event_time, "
            "sales_mode, processing_mode, reason FROM etl_reject"
        ) == [
            (
                "550001", "14001", "L1", 2, Decimal("1.500"), WHEN + timedelta(minutes=1),
                "Online", "Live", reason,
            )
        ]
        for table in (*DIMENSION_TABLES, "fact_sales", "live_cache"):
            assert _count(clean_db, table) == 0, f"{table} must stay empty"
        assert cache.count() == 0
        assert metrics.total(REJECTED) == 1
        assert metrics.total(LIVE_COMMITTED) == 0

    @pytest.mark.parametrize("mode", [PROCESSING_PERIODIC, PROCESSING_SHED])
    def test_unmatched_periodic_row_is_rejected_and_never_held(
        self, clean_db: Database, mode: str
    ) -> None:
        sink, _, metrics = _sink(clean_db)
        good = _staged(clean_db, 0, mode)
        bad = _staged(clean_db, 1, mode, product=False)
        sink.handle([_chunk(good, bad)])

        assert sink.held_count == 1
        assert clean_db.query("SELECT transaction_id, processing_mode, reason FROM etl_reject") == [
            (bad.transaction_id, mode, REASON_NO_PRODUCT)
        ]
        assert metrics.total(REJECTED) == 1

        sink.flush_held()
        flags = _staging_flags(clean_db)
        assert flags[good.staging_id] == STAGING_LOADED == 1
        assert flags[bad.staging_id] == STAGING_REJECTED
        assert STAGING_REJECTED not in (0, 1), "1 would claim the row reached the fact table"
        assert _fact_transactions(clean_db) == [good.transaction_id]

    def test_rejected_staging_row_is_not_picked_up_by_the_resume_reset(
        self, clean_db: Database
    ) -> None:
        """The reset in spec section 9 returns released rows with loaded = 0 to the
        stream. A rejected row returning would be rejected twice and break accounting."""
        sink, _, _ = _sink(clean_db)
        sink.handle([_chunk(_staged(clean_db, 0, product=False))])
        reset = clean_db.execute(
            "UPDATE periodic_sales_data p LEFT JOIN shed_table s ON s.staging_id = p.staging_id "
            "SET p.released = 0 WHERE p.released = 1 AND p.loaded = 0 AND s.staging_id IS NULL"
        )
        assert reset == 0

    def test_mixed_chunk_routes_every_row_once(self, clean_db: Database) -> None:
        sink, cache, metrics = _sink(clean_db)
        sink.handle(
            [
                _chunk(
                    _live(0),
                    _live(1, customer=False),
                    _staged(clean_db, 0),
                    _staged(clean_db, 1, customer=False),
                    _staged(clean_db, 2, PROCESSING_SHED),
                )
            ]
        )
        assert cache.count() == 1
        assert sink.held_count == 2
        assert _count(clean_db, "etl_reject") == 2
        assert _online_committed(clean_db) == 2
        assert metrics.total(LIVE_COMMITTED) == 1
        assert metrics.total(REJECTED) == 2


class TestLiveLatency:
    def test_latency_is_recorded_against_enqueued_at(self, clean_db: Database) -> None:
        sink, _, metrics = _sink(clean_db)
        sink.handle(
            [_chunk(_live(0, enqueued_at=100.0), _live(1, enqueued_at=99.0))], now=100.25
        )
        stats = metrics.take_hour()
        assert stats.live_latency_p50_ms == 1250
        assert stats.live_latency_p95_ms == 1250

    def test_clock_is_read_after_the_commit_when_not_given(
        self, clean_db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sink, _, metrics = _sink(clean_db)
        committed_when_read: list[int] = []

        def clock() -> float:
            committed_when_read.append(_online_committed(clean_db))
            return 500.0

        monkeypatch.setattr(sink_module.time, "monotonic", clock)
        sink.handle([_chunk(_live(0, enqueued_at=499.5))])
        monkeypatch.undo()
        assert committed_when_read == [1], "arrival-to-commit needs the time after the commit"
        assert metrics.take_hour().live_latency_p50_ms == 500

    def test_only_rows_that_committed_as_results_are_timed(self, clean_db: Database) -> None:
        sink, _, metrics = _sink(clean_db)
        sink.handle([_chunk(_live(0, customer=False, enqueued_at=1.0))], now=9.0)
        sink.handle([_chunk(_staged(clean_db, 0))], now=9.0)
        assert metrics.take_hour().live_latency_p50_ms is None

    def test_failed_commit_records_nothing(self, faulty: _Faulty) -> None:
        sink, _, metrics = _sink(faulty)
        faulty.arm("online_rows_committed", _raise_injected)
        with pytest.raises(RuntimeError):
            sink.handle([_chunk(_live(0, enqueued_at=1.0))], now=2.0)
        assert metrics.take_hour().live_latency_p50_ms is None


class TestMetrics:
    def test_direct_mode_counts_committed_and_loaded(self, clean_db: Database) -> None:
        sink, _, metrics = _sink(clean_db, LIVE_LOAD_DIRECT)
        sink.handle([_chunk(_live(0), _live(1), _live(2, product=False))])
        assert metrics.total(LIVE_COMMITTED) == 2
        assert metrics.total(LIVE_LOADED) == 2
        assert metrics.total(REJECTED) == 1

    def test_cache_mode_counts_committed_now_and_loaded_at_the_flush(
        self, clean_db: Database
    ) -> None:
        sink, _, metrics = _sink(clean_db, LIVE_LOAD_CACHE)
        sink.handle([_chunk(_live(0), _live(1))])
        assert metrics.total(LIVE_COMMITTED) == 2
        assert metrics.total(LIVE_LOADED) == 0
        sink.flush_cache_slice()
        assert metrics.total(LIVE_LOADED) == 2

    def test_counters_add_up_to_the_resume_offset(self, clean_db: Database) -> None:
        sink, _, metrics = _sink(clean_db)
        sink.handle([_chunk(_live(0), _live(1, customer=False)), _chunk(_live(2))])
        assert (
            metrics.total(LIVE_COMMITTED) + metrics.total(REJECTED)
            == _online_committed(clean_db)
        )


class TestEndToEndWarehouse:
    def test_live_and_periodic_rows_share_dimension_rows(self, clean_db: Database) -> None:
        """A customer who shows up in both forms must still be a single dimension row."""
        sink, _, _ = _sink(clean_db, LIVE_LOAD_DIRECT)
        online = _live(0)
        store = _staged(clean_db, 0)
        store.customer_id = online.customer_id
        store.customer = online.customer
        sink.handle([_chunk(online, store)])
        sink.flush_held()
        assert _count(clean_db, "fact_sales") == 2
        assert _count(clean_db, "dim_customer") == 1
        assert _count(clean_db, "dim_product") == 2
        assert _count(clean_db, "dim_store") == 2
