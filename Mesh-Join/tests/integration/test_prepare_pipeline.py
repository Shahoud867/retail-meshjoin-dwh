"""prepare_all and the two load functions on a tiny workbook and a real MySQL server."""

from __future__ import annotations

import csv
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import mysql.connector
import openpyxl
import pandas as pd
import pytest

from retail_dw.config import ConnectionConfig, Settings
from retail_dw.db import Database, MySqlError
from retail_dw.prepare import pipeline
from retail_dw.prepare.cleaning import SOURCE_COLUMNS, TEST_CANCELLATION, TEST_CUSTOMER
from retail_dw.prepare.master_data import build_customer_master, load_master_data
from retail_dw.prepare.pipeline import PreparationSummary, prepare_all
from retail_dw.prepare.split import load_staging

pytestmark = pytest.mark.integration

ONLINE_CART = 536365
PERIODIC_CART = 536370

WORKBOOK_ROWS: list[list[object]] = [
    # Online cart. The same product twice with names of different length.
    [ONLINE_CART, "85123A", "WHITE HEART ", 6, datetime(2010, 12, 1, 8, 26), 2.55, 12417, "Belgium"],
    [ONLINE_CART, "85123A", "HEART, WHITE", 2, datetime(2010, 12, 1, 8, 26), 2.55, 12417, "Spain"],
    [ONLINE_CART, 71053, 'THE "BEST" LANTERN', 6, datetime(2010, 12, 1, 8, 26), 3.39, 12417, "Spain"],
    # Periodic cart, written out of time order, with an integer-like float customer.
    [PERIODIC_CART, "22728", "ALARM CLOCK", 24, datetime(2011, 1, 5, 9, 0), 3.75, 12583.0, "Italy"],
    [PERIODIC_CART, "22729", "ALARM BELL", 1, datetime(2010, 12, 2, 9, 0), 0.001, 12583, "Italy"],
    # Removed by one test each.
    ["C536379", "D", "Discount", 1, datetime(2010, 12, 1, 9, 41), 27.5, 14527, "Spain"],
    [536381, "22730", "NO CUSTOMER", 1, datetime(2010, 12, 1, 9, 41), 1.0, None, "Spain"],
    [536382, "22731", "NEGATIVE", -5, datetime(2010, 12, 1, 9, 41), 1.0, 14527, "Spain"],
    [536383, "22732", "FREE", 5, datetime(2010, 12, 1, 9, 41), 0, 14527, "Spain"],
]

SELECT_STAGING = (
    "SELECT transaction_id, customer_id, product_id, quantity, unit_price, event_time, "
    "sales_mode, release_hour, released, loaded FROM periodic_sales_data ORDER BY staging_id"
)
ONE_CUSTOMER = [("1", "Male", "0-17", "Other", "A", "0", "Single")]


def write_raw_file(path: Path) -> None:
    workbook = openpyxl.Workbook()
    workbook.active.append(list(SOURCE_COLUMNS))
    for row in WORKBOOK_ROWS:
        workbook.active.append(row)
    workbook.save(path)


def read_csv(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.reader(handle))


@pytest.fixture
def raw_file(tmp_path: Path) -> Path:
    path = tmp_path / "raw.xlsx"
    write_raw_file(path)
    return path


@pytest.fixture
def prepared(
    clean_db: Database, temp_database: ConnectionConfig, raw_file: Path, tmp_path: Path
) -> tuple[PreparationSummary, list[str], Path]:
    lines: list[str] = []
    online_path = tmp_path / "out" / "online_sales_stream.csv"
    summary = prepare_all(Settings(), temp_database, raw_file, online_path, out=lines.append)
    return summary, lines, online_path


def test_summary_counts(prepared: tuple[PreparationSummary, list[str], Path]) -> None:
    summary, _, _ = prepared
    assert summary.before == 9
    assert summary.after == 5
    assert summary.online_rows == 3
    assert summary.periodic_rows == 2
    assert (summary.group_min, summary.group_max) == (0, 1)
    assert (summary.customers, summary.products) == (2, 4)
    assert (summary.customer_partitions, summary.product_partitions) == (1, 1)
    assert summary.removed_by_test[TEST_CANCELLATION] == 1
    assert summary.removed_by_test[TEST_CUSTOMER] == 1


def test_before_and_after_counts_are_printed(
    prepared: tuple[PreparationSummary, list[str], Path],
) -> None:
    _, lines, _ = prepared
    assert "Transactions before cleaning: 9" in lines
    assert "Transactions after cleaning: 5" in lines


def test_prints_go_to_stdout_by_default(
    clean_db: Database,
    temp_database: ConnectionConfig,
    raw_file: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    prepare_all(Settings(), temp_database, raw_file, tmp_path / "online.csv")
    assert "Transactions before cleaning: 9" in capsys.readouterr().out


def test_staging_rows_are_in_time_order_with_text_ids_and_exact_prices(
    prepared: tuple[PreparationSummary, list[str], Path], db: Database
) -> None:
    assert db.query(SELECT_STAGING) == [
        (
            "536370",
            "12583",
            "22729",
            1,
            Decimal("0.001"),
            datetime(2010, 12, 2, 9, 0),
            "Periodic",
            0,
            0,
            0,
        ),
        (
            "536370",
            "12583",
            "22728",
            24,
            Decimal("3.750"),
            datetime(2011, 1, 5, 9, 0),
            "Periodic",
            1,
            0,
            0,
        ),
    ]


def test_online_file_contents(prepared: tuple[PreparationSummary, list[str], Path]) -> None:
    _, _, online_path = prepared
    assert read_csv(online_path) == [
        [
            "transaction_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "event_time",
            "sales_mode",
        ],
        ["536365", "12417", "85123A", "6", "2.55", "2010-12-01 08:26:00", "Online"],
        ["536365", "12417", "85123A", "2", "2.55", "2010-12-01 08:26:00", "Online"],
        ["536365", "12417", "71053", "6", "3.39", "2010-12-01 08:26:00", "Online"],
    ]


def test_master_csv_files_sit_next_to_the_online_file_and_match_the_tables(
    prepared: tuple[PreparationSummary, list[str], Path], db: Database
) -> None:
    _, _, online_path = prepared
    customers = read_csv(online_path.parent / "customer_master_data.csv")
    products = read_csv(online_path.parent / "product_master_data.csv")
    assert [tuple(row) for row in customers[1:]] == db.query(
        "SELECT * FROM customer_master_data ORDER BY customer_id"
    )
    assert [tuple(row) for row in products[1:]] == db.query(
        "SELECT * FROM product_master_data ORDER BY product_id"
    )


def test_product_names_from_the_real_rule_survive_the_round_trip(
    prepared: tuple[PreparationSummary, list[str], Path], db: Database
) -> None:
    rows = dict(db.query("SELECT product_id, product_name FROM product_master_data"))
    assert rows["85123A"] == "HEART, WHITE"
    assert rows["71053"] == 'THE "BEST" LANTERN'


def test_city_category_comes_from_the_first_kept_row(
    prepared: tuple[PreparationSummary, list[str], Path], db: Database
) -> None:
    """The customer's rows say Belgium and then Spain, and the two give different
    categories. Recomputed in SQL so that no Python rule is reused."""
    [(category,)] = db.query(
        "SELECT city_category FROM customer_master_data WHERE customer_id = '12417'"
    )
    [(from_first, from_later)] = db.query(
        "SELECT ELT(CONV(LEFT(MD5('12417Belgium'), 8), 16, 10) MOD 3 + 1, 'A', 'B', 'C'), "
        "ELT(CONV(LEFT(MD5('12417Spain'), 8), 16, 10) MOD 3 + 1, 'A', 'B', 'C')"
    )
    assert from_first != from_later
    assert category == from_first


def test_prepared_flag_is_set(
    prepared: tuple[PreparationSummary, list[str], Path], db: Database
) -> None:
    [(flag, seq)] = db.query(
        "SELECT prepared, txn_seq FROM etl_progress WHERE writer = 'prepare'"
    )
    assert flag == 1
    assert seq >= 1


def test_preparation_touches_no_warehouse_or_runtime_table(
    prepared: tuple[PreparationSummary, list[str], Path], db: Database
) -> None:
    for table in (
        "dim_customer",
        "dim_product",
        "dim_supplier",
        "dim_store",
        "dim_time",
        "fact_sales",
        "shed_table",
        "live_cache",
        "etl_hourly_log",
        "etl_reject",
        "etl_live_status",
    ):
        assert db.query(f"SELECT COUNT(*) FROM {table}") == [(0,)], table


def test_prepared_flag_stays_unset_when_a_step_fails(
    clean_db: Database,
    temp_database: ConnectionConfig,
    raw_file: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing_load(_db: Database, _rows: object) -> int:
        raise RuntimeError("staging failed")

    monkeypatch.setattr(pipeline, "load_staging", failing_load)
    with pytest.raises(RuntimeError, match="staging failed"):
        prepare_all(Settings(), temp_database, raw_file, tmp_path / "o.csv", out=lambda _: None)
    assert clean_db.query("SELECT prepared FROM etl_progress WHERE writer = 'prepare'") == [(0,)]


def test_resume_is_refused_before_anything_is_read(
    temp_database: ConnectionConfig, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="--resume"):
        prepare_all(
            Settings(resume=True), temp_database, tmp_path / "missing.xlsx", tmp_path / "o.csv"
        )


def test_second_run_without_a_schema_rebuild_fails_loudly(
    prepared: tuple[PreparationSummary, list[str], Path],
    temp_database: ConnectionConfig,
    raw_file: Path,
) -> None:
    _, _, online_path = prepared
    with pytest.raises(MySqlError) as error:
        prepare_all(Settings(), temp_database, raw_file, online_path, out=lambda _: None)
    assert error.value.errno == 1062


# --- the load functions ------------------------------------------------------------


def statements_of_type_insert(db: Database) -> int:
    [(_, value)] = db.query("SHOW SESSION STATUS LIKE 'Com_insert'")
    return int(value)


def staging_rows(count: int) -> list[tuple]:
    moment = datetime(2011, 1, 1, 9, 0)
    return [
        ("536370", "12583", f"P{index}", 1, Decimal("1.250"), moment, "Periodic", index % 48)
        for index in range(count)
    ]


def test_staging_load_is_multi_row_not_row_by_row(clean_db: Database) -> None:
    rows = staging_rows(12_000)
    before = statements_of_type_insert(clean_db)
    assert load_staging(clean_db, rows) == 12_000
    # Three INSERT statements of 5,000 rows at most, and one progress upsert.
    assert statements_of_type_insert(clean_db) - before == 4
    assert clean_db.query("SELECT COUNT(*) FROM periodic_sales_data") == [(12_000,)]


def test_master_load_is_multi_row_not_row_by_row(clean_db: Database) -> None:
    customers = [
        (str(number), "Male", "0-17", "Other", "A", "0", "Single") for number in range(2_500)
    ]
    before = statements_of_type_insert(clean_db)
    load_master_data(clean_db, customers, [])
    # Three INSERT statements of 1,000 rows at most, and one progress upsert.
    assert statements_of_type_insert(clean_db) - before == 4
    assert clean_db.query("SELECT COUNT(*) FROM customer_master_data") == [(2_500,)]


def test_staging_id_follows_insertion_order(clean_db: Database) -> None:
    load_staging(clean_db, staging_rows(6_000))
    products = [row[0] for row in clean_db.query(
        "SELECT product_id FROM periodic_sales_data ORDER BY staging_id"
    )]
    assert products == [f"P{index}" for index in range(6_000)]


def fail_first_call_with_lost_connection(db: Database) -> Callable[[], int]:
    """Make the next executemany lose the connection, as a real server restart would."""
    real_executemany = db.executemany
    calls = {"count": 0}

    def flaky(sql: str, rows: object, batch_size: int = 1000) -> int:
        calls["count"] += 1
        if calls["count"] == 1:
            raise mysql.connector.errors.OperationalError(errno=2013, msg="Lost connection")
        return real_executemany(sql, rows, batch_size=batch_size)

    db.executemany = flaky  # type: ignore[method-assign]
    return lambda: calls["count"]


def test_staging_load_retry_still_has_rows_from_a_generator(clean_db: Database) -> None:
    """A retried transaction needs the rows again, and a generator gives them only once."""
    call_count = fail_first_call_with_lost_connection(clean_db)
    assert load_staging(clean_db, (row for row in staging_rows(10))) == 10
    assert call_count() == 2
    assert clean_db.query("SELECT COUNT(*) FROM periodic_sales_data") == [(10,)]


def test_failed_staging_load_leaves_no_rows(clean_db: Database) -> None:
    """The whole load is one transaction, so a bad row late on leaves nothing behind."""
    rows = staging_rows(6_000)
    rows[-1] = ("536370", "12583", "X" * 21, 1, Decimal("1"), datetime(2011, 1, 1), "Periodic", 0)
    with pytest.raises(MySqlError):
        load_staging(clean_db, rows)
    assert clean_db.query("SELECT COUNT(*) FROM periodic_sales_data") == [(0,)]


def test_failed_product_load_also_rolls_back_the_customers(clean_db: Database) -> None:
    too_long = ("X" * 21, "NAME", "Toys", "SUP01", "Northwind Supplies")
    with pytest.raises(MySqlError):
        load_master_data(clean_db, ONE_CUSTOMER, [too_long])
    assert clean_db.query("SELECT COUNT(*) FROM customer_master_data") == [(0,)]


def test_built_customers_pass_every_check_constraint(clean_db: Database) -> None:
    """The dim_customer CHECKs repeat the value lists, so a wrong list fails here."""
    frame = pd.DataFrame(
        [(str(number), "France") for number in range(12000, 12300)],
        columns=["customer_id", "country"],
        dtype=object,
    )
    customers = build_customer_master(frame)
    clean_db.executemany(
        "INSERT INTO dim_customer (customer_id, gender, age_group, occupation, city_category, "
        "stay_in_current_city_years, marital_status) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        customers,
    )
    assert clean_db.query("SELECT COUNT(*) FROM dim_customer") == [(300,)]
