"""The warehouse comparison has to catch real differences, or a pass against it means
nothing. These tests load small warehouses and damage them on purpose."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import replace

import pandas as pd
import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.db import Database
from tests.oracle.star_fixture import compare_warehouses, load_star

pytestmark = pytest.mark.integration

_SAMPLE_ROWS = 400
_FIRST_FACT_ROW = "SELECT MIN(sales_key) FROM fact_sales"


@pytest.fixture(scope="module")
def small_frame(raw_frame: pd.DataFrame) -> pd.DataFrame:
    """A slice from the start of the file that includes exact duplicate rows."""
    return raw_frame.head(_SAMPLE_ROWS)


@pytest.fixture
def warehouses(
    db_cfg: ConnectionConfig, small_frame: pd.DataFrame
) -> Iterator[tuple[ConnectionConfig, ConnectionConfig]]:
    """Two warehouses with equal content, whose surrogate keys and fact row order differ,
    which is the normal situation between the pipeline and the fixture."""
    suffix = uuid.uuid4().hex[:8]
    reference = replace(db_cfg, database=f"t_olap_cmp_a_{suffix}")
    candidate = replace(db_cfg, database=f"t_olap_cmp_b_{suffix}")
    try:
        load_star(reference, small_frame)
        load_star(candidate, small_frame.iloc[::-1], key_salt="other order")
        yield reference, candidate
    finally:
        with Database(db_cfg) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS `{reference.database}`")
            admin.execute(f"DROP DATABASE IF EXISTS `{candidate.database}`")


def test_equal_warehouses_match_even_with_different_surrogate_keys(
    warehouses: tuple[ConnectionConfig, ConnectionConfig],
) -> None:
    reference, candidate = warehouses
    with Database(reference) as left, Database(candidate) as right:
        sql = "SELECT product_id, product_key FROM dim_product"
        assert dict(left.query(sql)) != dict(right.query(sql))
    result = compare_warehouses(reference, candidate)
    assert result.identical, result.describe()
    assert result.rows_compared["fact_sales"] == (_SAMPLE_ROWS, _SAMPLE_ROWS)


def test_changed_revenue_is_found(warehouses: tuple[ConnectionConfig, ConnectionConfig]) -> None:
    reference, candidate = warehouses
    with Database(candidate) as db:
        first = db.query(_FIRST_FACT_ROW)[0][0]
        db.execute("UPDATE fact_sales SET revenue = revenue + 0.001 WHERE sales_key = %s", (first,))
    result = compare_warehouses(reference, candidate)
    assert not result.identical
    assert [d.table for d in result.differences] == ["fact_sales"]
    assert result.differences[0].only_in_reference == 1
    assert result.differences[0].only_in_candidate == 1


def test_one_lost_row_is_found(warehouses: tuple[ConnectionConfig, ConnectionConfig]) -> None:
    reference, candidate = warehouses
    with Database(candidate) as db:
        first = db.query(_FIRST_FACT_ROW)[0][0]
        db.execute("DELETE FROM fact_sales WHERE sales_key = %s", (first,))
    result = compare_warehouses(reference, candidate)
    assert result.differences[0].only_in_reference == 1
    assert result.differences[0].only_in_candidate == 0


def test_one_doubled_row_is_found_even_though_the_set_of_rows_is_unchanged(
    warehouses: tuple[ConnectionConfig, ConnectionConfig],
) -> None:
    reference, candidate = warehouses
    with Database(candidate) as db:
        db.execute(
            "INSERT INTO fact_sales (customer_key, product_key, supplier_key, store_key, time_key,"
            " transaction_id, sales_mode, processing_mode, quantity, revenue)"
            " SELECT customer_key, product_key, supplier_key, store_key, time_key,"
            " transaction_id, sales_mode, processing_mode, quantity, revenue"
            " FROM fact_sales ORDER BY sales_key LIMIT 1"
        )
    result = compare_warehouses(reference, candidate)
    assert result.differences[0].only_in_candidate == 1
    assert result.differences[0].only_in_reference == 0


def test_changed_dimension_attribute_is_found(
    warehouses: tuple[ConnectionConfig, ConnectionConfig],
) -> None:
    reference, candidate = warehouses
    with Database(candidate) as db:
        db.execute(
            "UPDATE dim_customer SET gender = IF(gender = 'Male', 'Female', 'Male') LIMIT 1"
        )
    result = compare_warehouses(reference, candidate)
    assert "dim_customer" in [d.table for d in result.differences]


def test_shed_periodic_rows_are_allowed_but_shed_online_rows_are_not(
    warehouses: tuple[ConnectionConfig, ConnectionConfig],
) -> None:
    reference, candidate = warehouses
    with Database(candidate) as db:
        periodic = db.execute(
            "UPDATE fact_sales SET processing_mode = 'Shed' WHERE sales_mode = 'Periodic' LIMIT 1"
        )
    assert periodic == 1, "the sample must contain a periodic row"
    result = compare_warehouses(reference, candidate)
    assert result.identical, result.describe()

    with Database(candidate) as db:
        db.execute(
            "UPDATE fact_sales SET processing_mode = 'Shed' WHERE sales_mode = 'Online' LIMIT 1"
        )
    result = compare_warehouses(reference, candidate)
    assert result.lineage_violations == (("Online", "Shed", 1),)
