"""MySqlPartitionSource against a real MySQL server.

The ids are the awkward ones from the real file: letters in both cases, spaces, and pairs
that differ only in case, such as 15056BL and 15056bl. Those pairs are what a case-insensitive
comparison would merge or skip, so they are placed on every possible page boundary.
"""

from __future__ import annotations

import json
import random
from collections.abc import Callable, Iterator, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.db import Database, MySqlError
from retail_dw.meshjoin.disk_buffer import (
    EmptyMasterDataError,
    MasterDataChangedError,
    MySqlPartitionSource,
)
from retail_dw.meshjoin.engine import MeshJoinEngine
from retail_dw.meshjoin.model import PROCESSING_LIVE, SALES_MODE_ONLINE, Transaction
from retail_dw.meshjoin.stream_buffer import StreamBuffer

pytestmark = pytest.mark.integration

AWKWARD_IDS = [
    "85123A",
    "85123a",
    "15056BL",
    "15056bl",
    "15056N",
    "15056n",
    "79323W",
    "79323w",
    "BANK CHARGES",
    "POST",
    "post",
    "DOT",
    "M",
    "m",
    "C2",
    "AMAZONFEE",
    "gift_0001_10",
    "gift_0001_20",
    "22423",
    "47566",
    "10002",
    "PADS",
]
TABLE = "t_master"
COLUMNS = ("product_id", "product_name")

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


def filler_ids(count: int) -> list[str]:
    return [f"{30000 + i}" for i in range(count)]


def name_of(key: str) -> str:
    return f"name of {key}"


@pytest.fixture
def make_table(db: Database) -> Iterator[Callable[[Sequence[str]], None]]:
    """Create the table with the same collation as the Master Data keys. Rows go in shuffled,
    so that any order a test sees has to come from the query and not the insert order."""

    def create(ids: Sequence[str]) -> None:
        db.execute(f"DROP TABLE IF EXISTS {TABLE}")
        db.execute(
            f"CREATE TABLE {TABLE} ("
            " product_id VARCHAR(20) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,"
            " product_name VARCHAR(255) NOT NULL"
            ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci"
        )
        shuffled = list(ids)
        random.Random(len(ids)).shuffle(shuffled)
        db.executemany(
            f"INSERT INTO {TABLE} (product_id, product_name) VALUES (%s, %s)",
            [(key, name_of(key)) for key in shuffled],
        )

    yield create
    db.execute(f"DROP TABLE IF EXISTS {TABLE}")


def source_on(db: Database, size: int = 500) -> MySqlPartitionSource:
    return MySqlPartitionSource(db, TABLE, "product_id", COLUMNS, size)


def read_cycle(source: MySqlPartitionSource) -> list[tuple]:
    rows: list[tuple] = []
    for _ in range(source.partitions):
        rows.extend(source.next_partition())
    return rows


@pytest.mark.parametrize("size", [1, 2, 3, 5, 7])
def test_every_row_once_per_cycle_with_a_boundary_at_every_awkward_id(
    db: Database, make_table: Callable[[Sequence[str]], None], size: int
) -> None:
    make_table(AWKWARD_IDS)
    source = source_on(db, size)
    expected = [(key, name_of(key)) for key in sorted(AWKWARD_IDS)]

    for _ in range(3):
        assert read_cycle(source) == expected


def test_every_row_once_per_cycle_at_the_real_partition_size(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    ids = AWKWARD_IDS + filler_ids(1315)
    make_table(ids)
    source = source_on(db)
    assert source.partitions == 3
    expected = [(key, name_of(key)) for key in sorted(ids)]

    for _ in range(3):
        cycle = [source.next_partition() for _ in range(3)]
        assert [len(partition) for partition in cycle] == [500, 500, 337]
        assert [row for partition in cycle for row in partition] == expected


def test_keyset_order_is_binary_order_under_utf8mb4_bin(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    make_table(AWKWARD_IDS)

    keys = [row[0] for row in read_cycle(source_on(db, 4))]

    server_order = [
        row[0] for row in db.query(f"SELECT product_id FROM {TABLE} ORDER BY product_id")
    ]
    assert keys == server_order
    assert keys == sorted(AWKWARD_IDS)
    # In binary order every upper case letter sorts before every lower case one. A
    # case-insensitive ordering would put these pairs side by side instead.
    assert keys.index("POST") < keys.index("gift_0001_10") < keys.index("m") < keys.index("post")


def test_the_join_keys_come_back_as_text_and_match_stream_ids_exactly(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    """A driver returning bytes for a binary collation would silently match nothing."""
    make_table(AWKWARD_IDS)

    rows = read_cycle(source_on(db))

    assert {type(row[0]) for row in rows} == {str}
    assert {row[0] for row in rows} == set(AWKWARD_IDS)


def test_wrap_around_restarts_at_the_first_partition(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    make_table(AWKWARD_IDS)
    source = source_on(db, 10)
    assert source.partitions == 3
    assert source.position == 0
    ordered = [(key, name_of(key)) for key in sorted(AWKWARD_IDS)]

    positions = []
    for read in range(7):
        partition = source.next_partition()
        positions.append(source.position)
        page = read % 3
        assert partition == ordered[page * 10 : page * 10 + 10]

    assert positions == [1, 2, 3, 1, 2, 3, 1]


@pytest.mark.parametrize(
    ("rows", "partitions", "last_size"),
    [
        (1, 1, 1),
        (499, 1, 499),
        (500, 1, 500),
        (501, 2, 1),
        (1000, 2, 500),
        (1001, 3, 1),
    ],
)
def test_partition_count_is_the_ceiling_and_divisible_tables_have_no_empty_partition(
    db: Database,
    make_table: Callable[[Sequence[str]], None],
    rows: int,
    partitions: int,
    last_size: int,
) -> None:
    make_table(filler_ids(rows))
    source = source_on(db)

    assert source.partitions == partitions

    sizes = [len(source.next_partition()) for _ in range(partitions)]
    assert sizes == [500] * (partitions - 1) + [last_size]
    # The read after the last partition is the first partition again, never an empty one.
    assert source.next_partition()[0][0] == "30000"
    assert source.position == 1


def test_an_empty_table_raises(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    make_table([])
    with pytest.raises(EmptyMasterDataError, match=TABLE):
        source_on(db)


class StatementSpy:
    """Stands in for Database and records the page reads made through it."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self.statements: list[tuple[str, Any]] = []

    def query(self, sql: str, params: Any = None) -> list[tuple]:
        self.statements.append((sql, params))
        return self._db.query(sql, params)

    def run_transaction(self, fn: Callable[[Any], Any], writer: str | None = None) -> Any:
        return self._db.run_transaction(lambda _db: fn(self), writer)


def test_every_partition_is_read_from_disk_with_nothing_cached(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    """Change a row between cycles: the next cycle must show it. Also counts the statements,
    so that a cache refreshed some other way could not pass either."""
    make_table(AWKWARD_IDS)
    spy = StatementSpy(db)
    source = MySqlPartitionSource(spy, TABLE, "product_id", COLUMNS, 10)  # type: ignore[arg-type]
    assert read_cycle(source)[0] == ("10002", name_of("10002"))
    spy.statements.clear()

    db.execute(f"UPDATE {TABLE} SET product_name = 'changed' WHERE product_id = '10002'")
    cycle = read_cycle(source)

    assert cycle[0] == ("10002", "changed")
    assert len(spy.statements) == source.partitions


def test_reads_are_keyset_and_use_the_primary_key_without_a_sort_or_offset(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    make_table(AWKWARD_IDS + filler_ids(1200))
    spy = StatementSpy(db)
    source = MySqlPartitionSource(spy, TABLE, "product_id", COLUMNS)  # type: ignore[arg-type]
    first_page = source.next_partition()
    spy.statements.clear()

    source.next_partition()

    assert len(spy.statements) == 1
    sql, params = spy.statements[0]
    assert "OFFSET" not in sql.upper()
    assert params == (first_page[-1][0],)
    plan = json.loads(db.query("EXPLAIN FORMAT=JSON " + sql, params)[0][0])
    ordering = plan["query_block"]["ordering_operation"]
    assert ordering["using_filesort"] is False
    assert ordering["table"]["key"] == "PRIMARY"
    assert ordering["table"]["access_type"] == "range"


def test_a_killed_session_costs_a_reconnect_and_no_row(
    db: Database,
    make_table: Callable[[Sequence[str]], None],
    temp_database: ConnectionConfig,
) -> None:
    make_table(AWKWARD_IDS)
    source = source_on(db, 5)
    expected = [(key, name_of(key)) for key in sorted(AWKWARD_IDS)]

    rows = source.next_partition()
    with Database(temp_database) as admin:
        admin.execute(f"KILL {db.connection_id}")
    for _ in range(source.partitions - 1):
        rows.extend(source.next_partition())

    assert rows == expected
    assert source.position == source.partitions


def test_a_failed_read_does_not_move_the_cursor(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    make_table(AWKWARD_IDS)
    source = source_on(db, 5)
    expected = [(key, name_of(key)) for key in sorted(AWKWARD_IDS)]
    assert source.next_partition() == expected[:5]

    db.execute(f"RENAME TABLE {TABLE} TO {TABLE}_away")
    try:
        with pytest.raises(MySqlError, match="doesn't exist"):
            source.next_partition()
        assert source.position == 1
    finally:
        db.execute(f"RENAME TABLE {TABLE}_away TO {TABLE}")

    assert source.next_partition() == expected[5:10]
    assert source.position == 2


def test_rows_removed_during_the_run_stop_it_loudly(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    make_table(filler_ids(9))
    source = source_on(db, 3)
    source.next_partition()

    db.execute(f"DELETE FROM {TABLE} WHERE product_id > '30004'")

    with pytest.raises(MasterDataChangedError, match="partition 2 of 3"):
        source.next_partition()
    assert source.position == 1


def test_a_table_emptied_during_the_run_stops_it_loudly(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    make_table(filler_ids(4))
    source = source_on(db, 3)
    source.next_partition()

    db.execute(f"DELETE FROM {TABLE}")

    with pytest.raises(MasterDataChangedError, match="has 0 rows"):
        source.next_partition()


def test_rows_added_to_a_short_last_partition_stop_it_loudly(
    db: Database, make_table: Callable[[Sequence[str]], None]
) -> None:
    make_table(filler_ids(4))
    source = source_on(db, 3)
    source.next_partition()

    db.execute(f"INSERT INTO {TABLE} VALUES ('30009', 'late')")

    with pytest.raises(MasterDataChangedError, match="partition 2 of 2"):
        source.next_partition()


@pytest.mark.parametrize(
    ("table", "key_column", "columns"),
    [
        ("t`x", "product_id", COLUMNS),
        ("t_master; DROP TABLE t_master", "product_id", COLUMNS),
        ("other.t_master", "product_id", COLUMNS),
        ("t master", "product_id", COLUMNS),
        ("", "product_id", COLUMNS),
        ("t_master\n", "product_id", COLUMNS),
        ("t_master", "product_id`", ("product_id`", "product_name")),
        ("t_master", "product_id", ("product_id", "product_name, SLEEP(5)")),
    ],
)
def test_unsafe_identifiers_are_refused_before_any_sql_runs(
    db: Database,
    make_table: Callable[[Sequence[str]], None],
    table: str,
    key_column: str,
    columns: tuple[str, ...],
) -> None:
    make_table(AWKWARD_IDS)
    spy = StatementSpy(db)

    with pytest.raises(ValueError, match="safe"):
        MySqlPartitionSource(spy, table, key_column, columns)  # type: ignore[arg-type]

    assert spy.statements == []
    assert db.query(f"SELECT COUNT(*) FROM {TABLE}") == [(len(AWKWARD_IDS),)]


def test_the_key_column_must_be_among_the_columns_read(db: Database) -> None:
    with pytest.raises(ValueError, match="key column"):
        MySqlPartitionSource(db, TABLE, "product_id", ("product_name",))


@pytest.mark.parametrize("size", [0, -5])
def test_a_bad_partition_size_is_refused(db: Database, size: int) -> None:
    with pytest.raises(ValueError, match="size"):
        MySqlPartitionSource(db, TABLE, "product_id", COLUMNS, size)


def test_engine_joins_the_real_master_tables_through_mysql(clean_db: Database) -> None:
    """The whole path on the real schema with the real table sizes: Nc = 9 and Np = 8, with
    every tuple arriving in the column order the rest of the program relies on."""
    customers = [
        (f"{12346 + i}", "Female", "18-25", "Student", "A", "1", "Single") for i in range(4338)
    ]
    products = [
        (key, name_of(key), "Toys", "SUP01", "Northwind Supplies")
        for key in AWKWARD_IDS + filler_ids(3665 - len(AWKWARD_IDS))
    ]
    clean_db.executemany(
        f"INSERT INTO customer_master_data ({', '.join(CUSTOMER_COLUMNS)})"
        " VALUES (%s, %s, %s, %s, %s, %s, %s)",
        customers,
    )
    clean_db.executemany(
        f"INSERT INTO product_master_data ({', '.join(PRODUCT_COLUMNS)})"
        " VALUES (%s, %s, %s, %s, %s)",
        products,
    )
    engine = MeshJoinEngine(
        StreamBuffer(),
        MySqlPartitionSource(clean_db, "customer_master_data", "customer_id", CUSTOMER_COLUMNS),
        MySqlPartitionSource(clean_db, "product_master_data", "product_id", PRODUCT_COLUMNS),
    )
    assert (engine.customer_partitions, engine.product_partitions) == (9, 8)
    stream = [
        Transaction(
            transaction_id=str(536365 + i),
            customer_id=customers[i][0],
            product_id=products[i % len(products)][0],
            quantity=1,
            unit_price=Decimal("1.250"),
            event_time=datetime(2011, 1, 1, 9, 0),
            sales_mode=SALES_MODE_ONLINE,
            processing_mode=PROCESSING_LIVE,
        )
        for i in range(len(customers))
    ]
    stream.append(
        Transaction(
            "999999",
            customers[0][0],
            "NO-SUCH-PRODUCT",
            1,
            Decimal("1.250"),
            datetime(2011, 1, 1, 9, 0),
            SALES_MODE_ONLINE,
            PROCESSING_LIVE,
        )
    )

    output: list[Transaction] = []
    position = 0
    while position < len(stream) or engine.has_work():
        position += engine.stream_buffer.put_online(stream[position:], timeout=0)
        for chunk in engine.run_iteration():
            output.extend(chunk.transactions)

    assert output == stream
    for index, transaction in enumerate(stream[:-1]):
        assert transaction.customer == customers[index]
        assert transaction.product == products[index % len(products)]
    assert stream[-1].customer == customers[0]
    assert stream[-1].product is None
    assert not stream[-1].is_enriched
