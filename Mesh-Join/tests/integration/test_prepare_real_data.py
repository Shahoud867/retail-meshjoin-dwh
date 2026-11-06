"""The whole preparation on the real file, against the reference totals of the build spec.

Slow: reading the workbook alone takes most of a minute, so prepare_all runs once for
the module and every test looks at what it left behind.
"""

from __future__ import annotations

import csv
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from retail_dw.config import ConnectionConfig, Settings
from retail_dw.db import Database, locate_create_dw_script, run_sql_script
from retail_dw.prepare.cleaning import (
    TEST_CANCELLATION,
    TEST_CUSTOMER,
    TEST_PRICE,
    TEST_QUANTITY,
)
from retail_dw.prepare.pipeline import PreparationSummary, prepare_all
from retail_dw.prepare.rules import sales_mode_for, stable, store_id_for

pytestmark = [pytest.mark.integration, pytest.mark.slow]

RAW_FILE = Path(__file__).resolve().parents[3] / "Data" / "raw" / "Online Retail.xlsx"

ONLINE_HEADER = (
    "transaction_id,customer_id,product_id,quantity,unit_price,event_time,sales_mode\n"
)

_SELECT_STAGING = (
    "SELECT transaction_id, customer_id, product_id, quantity, unit_price, event_time, "
    "sales_mode, release_hour, staging_id FROM periodic_sales_data ORDER BY staging_id"
)


@dataclass(frozen=True)
class Sale:
    transaction_id: str
    customer_id: str
    product_id: str
    quantity: int
    unit_price: Decimal
    event_time: datetime
    sales_mode: str

    @property
    def revenue(self) -> Decimal:
        return self.quantity * self.unit_price

    @property
    def store_id(self) -> int:
        return store_id_for(self.transaction_id, self.sales_mode)


@dataclass(frozen=True)
class Run:
    summary: PreparationSummary
    lines: list[str]
    online_path: Path


@pytest.fixture(scope="module")
def run(temp_database: ConnectionConfig, tmp_path_factory: pytest.TempPathFactory) -> Run:
    if not RAW_FILE.is_file():
        pytest.skip(f"real dataset not available: {RAW_FILE}")
    with Database(temp_database) as session:
        run_sql_script(session, locate_create_dw_script())
    lines: list[str] = []
    online_path = tmp_path_factory.mktemp("prepare_real") / "online_sales_stream.csv"
    summary = prepare_all(Settings(), temp_database, RAW_FILE, online_path, out=lines.append)
    return Run(summary, lines, online_path)


@pytest.fixture(scope="module")
def online_sales(run: Run) -> list[Sale]:
    with run.online_path.open(newline="", encoding="utf-8") as handle:
        return [
            Sale(
                row["transaction_id"],
                row["customer_id"],
                row["product_id"],
                int(row["quantity"]),
                Decimal(row["unit_price"]),
                datetime.strptime(row["event_time"], "%Y-%m-%d %H:%M:%S"),
                row["sales_mode"],
            )
            for row in csv.DictReader(handle)
        ]


@pytest.fixture(scope="module")
def staging_rows(run: Run, temp_database: ConnectionConfig) -> list[tuple]:
    with Database(temp_database) as session:
        return session.query(_SELECT_STAGING)


@pytest.fixture(scope="module")
def periodic_sales(staging_rows: list[tuple]) -> list[Sale]:
    return [Sale(*row[:7]) for row in staging_rows]


@pytest.fixture(scope="module")
def all_sales(online_sales: list[Sale], periodic_sales: list[Sale]) -> list[Sale]:
    return online_sales + periodic_sales


@pytest.fixture(scope="module")
def session(run: Run, temp_database: ConnectionConfig) -> Iterator[Database]:
    with Database(temp_database) as database:
        yield database


def total_revenue(sales: list[Sale]) -> Decimal:
    return sum((sale.revenue for sale in sales), Decimal(0))


def total_quantity(sales: list[Sale]) -> int:
    return sum(sale.quantity for sale in sales)


# --- cleaning counts ---------------------------------------------------------------


def test_before_and_after(run: Run) -> None:
    assert run.summary.before == 541_909
    assert run.summary.after == 397_884


def test_removed_by_each_test(run: Run) -> None:
    assert run.summary.removed_by_test == {
        TEST_CANCELLATION: 9_288,
        TEST_QUANTITY: 10_624,
        TEST_PRICE: 2_517,
        TEST_CUSTOMER: 135_080,
    }


def test_counts_are_printed_with_their_labels(run: Run) -> None:
    assert "Transactions before cleaning: 541,909" in run.lines
    assert "Transactions after cleaning: 397,884" in run.lines
    assert "Shopping carts: 18,532" in run.lines
    assert "Customer Master Data: 4,338 tuples, 9 partitions" in run.lines
    assert "Product Master Data: 3,665 tuples, 8 partitions" in run.lines
    assert any("119,144" in line and "2,482" in line and "2,483" in line for line in run.lines)


def test_carts(all_sales: list[Sale]) -> None:
    assert len({sale.transaction_id for sale in all_sales}) == 18_532


def test_exact_duplicate_rows_are_kept(all_sales: list[Sale]) -> None:
    """Measured on the seven fields the project keeps. Two more rows differ from their
    twin only in Description, so the count over all eight raw columns is 5,192."""
    assert len(all_sales) - len(set(all_sales)) == 5_194


# --- master data -------------------------------------------------------------------


def test_customer_partitions(run: Run, session: Database) -> None:
    assert run.summary.customers == 4_338
    assert run.summary.customer_partitions == 9
    assert session.query("SELECT COUNT(*) FROM customer_master_data") == [(4_338,)]
    assert run.summary.customers % 500 == 338


def test_product_partitions(run: Run, session: Database) -> None:
    assert run.summary.products == 3_665
    assert run.summary.product_partitions == 8
    assert session.query("SELECT COUNT(*) FROM product_master_data") == [(3_665,)]
    assert run.summary.products % 500 == 165


def test_master_data_covers_exactly_the_ids_on_the_transactions(
    all_sales: list[Sale], session: Database
) -> None:
    customers = {row[0] for row in session.query("SELECT customer_id FROM customer_master_data")}
    products = {row[0] for row in session.query("SELECT product_id FROM product_master_data")}
    assert customers == {sale.customer_id for sale in all_sales}
    assert products == {sale.product_id for sale in all_sales}


def test_customer_rules_recomputed_inside_mysql(session: Database) -> None:
    """MySQL's MD5 and ELT share no code with the Python rules or their value lists."""
    wrong = session.query(
        """
        SELECT COUNT(*) FROM customer_master_data WHERE
            gender <> ELT(CONV(LEFT(MD5(customer_id), 8), 16, 10) MOD 2 + 1, 'Male', 'Female')
         OR age_group <> ELT(CONV(LEFT(MD5(CONCAT(customer_id, 'age')), 8), 16, 10) MOD 7 + 1,
                '0-17', '18-25', '26-35', '36-45', '46-50', '51-55', '55+')
         OR occupation <> ELT(CONV(LEFT(MD5(CONCAT(customer_id, 'occ')), 8), 16, 10) MOD 10 + 1,
                'Student', 'Engineer', 'Teacher', 'Executive', 'Clerk',
                'Healthcare', 'Retail', 'Homemaker', 'Artist', 'Other')
         OR stay_in_current_city_years <> ELT(
                CONV(LEFT(MD5(CONCAT(customer_id, 'stay')), 8), 16, 10) MOD 5 + 1,
                '0', '1', '2', '3', '4+')
         OR marital_status <> ELT(CONV(LEFT(MD5(CONCAT(customer_id, 'mar')), 8), 16, 10) MOD 2 + 1,
                'Single', 'Married')
        """
    )
    assert wrong == [(0,)]


def test_product_rules_recomputed_inside_mysql(session: Database) -> None:
    wrong = session.query(
        """
        SELECT COUNT(*) FROM product_master_data WHERE
            product_category <> ELT(CONV(LEFT(MD5(CONCAT(product_id, 'cat')), 8), 16, 10) MOD 10 + 1,
                'Home Decor', 'Kitchen', 'Stationery', 'Toys', 'Seasonal Gifts',
                'Bags', 'Jewelry', 'Garden', 'Party', 'Other')
         OR supplier_id <> ELT(CONV(LEFT(MD5(product_id), 8), 16, 10) MOD 5 + 1,
                'SUP01', 'SUP02', 'SUP03', 'SUP04', 'SUP05')
         OR supplier_name <> ELT(CONV(LEFT(MD5(product_id), 8), 16, 10) MOD 5 + 1,
                'Northwind Supplies', 'Atlantic Gifts Ltd', 'Midlands Wholesale',
                'Celtic Craft Co', 'EuroPack Partners')
        """
    )
    assert wrong == [(0,)]


@pytest.mark.parametrize(
    ("customer_id", "first_country", "later_country"),
    [("12417", "Belgium", "Spain"), ("12422", "Australia", "Switzerland"), ("12457", "Switzerland", "Cyprus")],
)
def test_multi_country_customers_use_the_first_country(
    session: Database, customer_id: str, first_country: str, later_country: str
) -> None:
    """For these three customers the later country would give a different category."""
    [(category,)] = session.query(
        "SELECT city_category FROM customer_master_data WHERE customer_id = %s", (customer_id,)
    )
    assert category == "ABC"[stable(customer_id + first_country, 3)]
    assert category != "ABC"[stable(customer_id + later_country, 3)]


@pytest.mark.parametrize(
    ("product_id", "name"),
    [
        # A tie of real names: the first in the file wins.
        ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER"),
        # The second is longer only because of a trailing space.
        ("22986", "GINGHAM ROSE WRAP"),
        # Raw lengths tie, but the first name is shorter once trimmed.
        ("21818", "GLITTER HEART DECORATION"),
        # Raw longest has a trailing space, and after trimming it ties with an earlier name.
        ("22776", "CAKESTAND, 3 TIER, LOVEHEART"),
    ],
)
def test_product_name_rule_on_real_cases(session: Database, product_id: str, name: str) -> None:
    assert session.query(
        "SELECT product_name FROM product_master_data WHERE product_id = %s", (product_id,)
    ) == [(name,)]


def test_no_product_name_has_edge_whitespace_or_is_empty(session: Database) -> None:
    rows = session.query("SELECT product_name FROM product_master_data")
    assert all(name == name.strip() and name for (name,) in rows)


def test_master_csv_files_match_the_tables(run: Run, session: Database) -> None:
    for name, table, key in (
        ("customer_master_data.csv", "customer_master_data", "customer_id"),
        ("product_master_data.csv", "product_master_data", "product_id"),
    ):
        with (run.online_path.parent / name).open(newline="", encoding="utf-8") as handle:
            file_rows = [tuple(row) for row in csv.reader(handle)][1:]
        assert file_rows == session.query(f"SELECT * FROM {table} ORDER BY {key}")


# --- online and periodic rows ------------------------------------------------------


def test_online_totals_from_the_csv(run: Run, online_sales: list[Sale]) -> None:
    assert run.summary.online_rows == 278_740
    assert len(online_sales) == 278_740
    assert total_quantity(online_sales) == 3_526_585
    assert total_revenue(online_sales) == Decimal("6075186.073")


def test_periodic_totals_from_staging(run: Run, session: Database) -> None:
    assert run.summary.periodic_rows == 119_144
    assert session.query(
        "SELECT COUNT(*), SUM(quantity), SUM(quantity * unit_price) FROM periodic_sales_data"
    ) == [(119_144, Decimal(1_641_227), Decimal("2836221.831"))]


def test_grand_totals(all_sales: list[Sale]) -> None:
    assert len(all_sales) == 397_884
    assert total_quantity(all_sales) == 5_167_812
    assert total_revenue(all_sales) == Decimal("8911407.904")


def test_totals_by_year(all_sales: list[Sale]) -> None:
    for year, rows, revenue in ((2010, 26_157, "572713.890"), (2011, 371_727, "8338694.014")):
        sales = [sale for sale in all_sales if sale.event_time.year == year]
        assert len(sales) == rows
        assert total_revenue(sales) == Decimal(revenue)


def test_store_rule_row_counts_and_revenue(all_sales: list[Sale]) -> None:
    assert Counter(sale.store_id for sale in all_sales) == {
        1: 278_740,
        2: 21_053,
        3: 37_302,
        4: 19_850,
        5: 40_939,
    }
    revenue = {
        1: "6075186.073",
        2: "463139.470",
        3: "868061.800",
        4: "632043.680",
        5: "872976.881",
    }
    for store_id, expected in revenue.items():
        sales = [sale for sale in all_sales if sale.store_id == store_id]
        assert total_revenue(sales) == Decimal(expected), store_id


def test_store_rule_recomputed_inside_mysql_gives_the_same_counts(session: Database) -> None:
    rows = session.query(
        "SELECT 2 + CONV(LEFT(MD5(transaction_id), 8), 16, 10) MOD 4 AS store, COUNT(*) "
        "FROM periodic_sales_data GROUP BY store ORDER BY store"
    )
    assert [(int(store), count) for store, count in rows] == [
        (2, 21_053),
        (3, 37_302),
        (4, 19_850),
        (5, 40_939),
    ]


def test_sales_mode_rule_holds_on_every_row(
    online_sales: list[Sale], periodic_sales: list[Sale], session: Database
) -> None:
    assert {sale.sales_mode for sale in online_sales} == {"Online"}
    assert {sale.sales_mode for sale in periodic_sales} == {"Periodic"}
    assert all(sales_mode_for(sale.transaction_id) == "Online" for sale in online_sales)
    assert session.query(
        "SELECT COUNT(*) FROM periodic_sales_data "
        "WHERE CONV(LEFT(MD5(transaction_id), 8), 16, 10) MOD 10 < 7"
    ) == [(0,)]


def test_no_cart_is_in_both_forms(online_sales: list[Sale], periodic_sales: list[Sale]) -> None:
    online = {sale.transaction_id for sale in online_sales}
    assert online.isdisjoint(sale.transaction_id for sale in periodic_sales)


def test_build_spec_vector_carts_are_on_the_right_side(
    online_sales: list[Sale], periodic_sales: list[Sale]
) -> None:
    assert online_sales[0].transaction_id == "536365"
    assert any(sale.transaction_id == "536370" for sale in periodic_sales)
    assert not any(sale.transaction_id == "536370" for sale in online_sales)


def test_distinct_minutes_match_the_expected_time_dimension(all_sales: list[Sale]) -> None:
    assert len({sale.event_time for sale in all_sales}) == 17_282


def test_event_times_are_whole_minutes_in_the_published_range(all_sales: list[Sale]) -> None:
    assert all(sale.event_time.second == 0 for sale in all_sales)
    assert min(sale.event_time for sale in all_sales) == datetime(2010, 12, 1, 8, 26)
    assert max(sale.event_time for sale in all_sales) == datetime(2011, 12, 9, 12, 50)


# --- periodic groups ---------------------------------------------------------------


def test_group_sizes(run: Run, session: Database) -> None:
    rows = session.query(
        "SELECT release_hour, COUNT(*) FROM periodic_sales_data "
        "GROUP BY release_hour ORDER BY release_hour"
    )
    assert rows == [(hour, 2_483 if hour < 8 else 2_482) for hour in range(48)]
    assert (run.summary.group_min, run.summary.group_max) == (2_482, 2_483)


def test_staging_order_is_event_time_order(staging_rows: list[tuple]) -> None:
    times = [row[5] for row in staging_rows]
    assert times == sorted(times)
    hours = [row[7] for row in staging_rows]
    assert hours == sorted(hours)


def test_groups_do_not_overlap_in_time(session: Database) -> None:
    rows = session.query(
        "SELECT MIN(event_time), MAX(event_time) FROM periodic_sales_data "
        "GROUP BY release_hour ORDER BY release_hour"
    )
    for (_, latest), (earliest, _) in zip(rows, rows[1:]):
        assert latest <= earliest


def test_every_staging_row_starts_unreleased_and_unloaded(session: Database) -> None:
    assert session.query(
        "SELECT released, loaded, COUNT(*) FROM periodic_sales_data GROUP BY released, loaded"
    ) == [(0, 0, 119_144)]


# --- text ids ----------------------------------------------------------------------


def test_no_cancellation_id_anywhere(all_sales: list[Sale]) -> None:
    assert not any(sale.transaction_id.startswith("C") for sale in all_sales)


def test_no_point_zero_in_any_id(all_sales: list[Sale], session: Database) -> None:
    for sale in all_sales:
        assert not sale.transaction_id.endswith(".0")
        assert not sale.customer_id.endswith(".0")
        assert not sale.product_id.endswith(".0")
    assert session.query(
        "SELECT COUNT(*) FROM customer_master_data WHERE customer_id LIKE '%.0'"
    ) == [(0,)]
    assert session.query(
        "SELECT COUNT(*) FROM product_master_data WHERE product_id LIKE '%.0'"
    ) == [(0,)]
    assert session.query(
        "SELECT COUNT(*) FROM periodic_sales_data "
        "WHERE transaction_id LIKE '%.0' OR customer_id LIKE '%.0' OR product_id LIKE '%.0'"
    ) == [(0,)]


def test_customer_ids_are_all_digits(all_sales: list[Sale]) -> None:
    assert all(sale.customer_id.isdigit() for sale in all_sales)


# --- files and the progress flag ---------------------------------------------------


def test_csv_files_are_lf_utf8_without_bom(run: Run) -> None:
    for name in (
        run.online_path.name,
        "customer_master_data.csv",
        "product_master_data.csv",
    ):
        data = (run.online_path.parent / name).read_bytes()
        assert b"\r" not in data, name
        assert not data.startswith(b"\xef\xbb\xbf"), name
        data.decode("utf-8")


def test_online_file_header(run: Run) -> None:
    with run.online_path.open(newline="", encoding="utf-8") as handle:
        assert handle.readline() == ONLINE_HEADER


def test_prepared_flag(session: Database) -> None:
    assert session.query("SELECT prepared FROM etl_progress WHERE writer = 'prepare'") == [(1,)]
