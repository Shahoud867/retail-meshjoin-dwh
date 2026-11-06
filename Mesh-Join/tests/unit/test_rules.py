"""The fixed rules: MD5 vectors, canonical ids, value lists and time parts."""

from __future__ import annotations

import hashlib
import itertools
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from retail_dw.meshjoin.model import SALES_MODE_ONLINE, SALES_MODE_PERIODIC
from retail_dw.prepare import rules
from retail_dw.prepare.master_data import (
    AGE_GROUPS,
    CITY_CATEGORIES,
    GENDERS,
    MARITAL_STATUSES,
    OCCUPATIONS,
    PRODUCT_CATEGORIES,
    STAY_YEARS,
    SUPPLIERS,
)
from retail_dw.prepare.rules import (
    STORE_LIST,
    canonical_id,
    md5_number,
    sales_mode_for,
    stable,
    store_id_for,
    time_parts,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DIM_TIME_COLUMNS = [
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
]


def test_public_names_are_the_pinned_contract() -> None:
    assert set(rules.__all__) == {
        "canonical_id",
        "md5_number",
        "stable",
        "sales_mode_for",
        "store_id_for",
        "time_parts",
        "STORE_LIST",
    }


def test_rules_do_not_import_pandas() -> None:
    """The run-time load path uses these rules, and pandas is for preparation only."""
    code = (
        "import sys, retail_dw.prepare.rules; "
        "sys.exit('pandas was imported' if 'pandas' in sys.modules else 0)"
    )
    subprocess.run([sys.executable, "-c", code], cwd=PROJECT_ROOT, check=True)


# --- MD5 rules ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("transaction_id", "first_8_hex", "number", "remainder", "mode", "store"),
    [
        ("536365", "3e6d49ce", 1047349710, 0, SALES_MODE_ONLINE, 1),
        ("536370", "2d918f31", 764514097, 7, SALES_MODE_PERIODIC, 3),
    ],
)
def test_build_spec_vectors(
    transaction_id: str, first_8_hex: str, number: int, remainder: int, mode: str, store: int
) -> None:
    assert hashlib.md5(transaction_id.encode()).hexdigest()[:8] == first_8_hex
    assert md5_number(transaction_id) == number
    assert number % 10 == remainder
    assert sales_mode_for(transaction_id) == mode
    assert store_id_for(transaction_id, mode) == store


def test_md5_number_encodes_the_text_as_utf8() -> None:
    expected = int(hashlib.md5("café".encode("utf-8")).hexdigest()[:8], 16)
    assert md5_number("café") == expected
    assert md5_number("café") != int(
        hashlib.md5("café".encode("latin-1")).hexdigest()[:8], 16
    )


@pytest.mark.parametrize("n", [2, 3, 5, 7, 10])
def test_stable_is_md5_number_modulo_n(n: int) -> None:
    for text in ("17850", "17850age", "85123Acat"):
        assert stable(text, n) == md5_number(text) % n
        assert 0 <= stable(text, n) < n


@pytest.mark.parametrize(
    ("remainder", "mode"),
    [(value, SALES_MODE_ONLINE) for value in range(7)]
    + [(value, SALES_MODE_PERIODIC) for value in range(7, 10)],
)
def test_sales_mode_for_every_remainder(remainder: int, mode: str) -> None:
    transaction_id = next(
        text for text in map(str, itertools.count(536000)) if md5_number(text) % 10 == remainder
    )
    assert sales_mode_for(transaction_id) == mode


@pytest.mark.parametrize("remainder", range(4))
def test_store_rule_for_every_physical_store(remainder: int) -> None:
    transaction_id = next(
        text
        for text in map(str, itertools.count(536000))
        if sales_mode_for(text) == SALES_MODE_PERIODIC and md5_number(text) % 4 == remainder
    )
    assert store_id_for(transaction_id, SALES_MODE_PERIODIC) == 2 + remainder


def test_online_cart_is_always_store_one() -> None:
    online_carts = [
        text for text in map(str, range(536000, 536050)) if sales_mode_for(text) == SALES_MODE_ONLINE
    ]
    assert online_carts
    assert {store_id_for(text, SALES_MODE_ONLINE) for text in online_carts} == {1}


def test_store_list_is_the_five_stores_of_the_pdf() -> None:
    assert STORE_LIST == {
        1: ("Online", "Web"),
        2: ("London Bridge", "London"),
        3: ("Manchester Arndale", "Manchester"),
        4: ("Birmingham Bullring", "Birmingham"),
        5: ("Edinburgh Princes", "Edinburgh"),
    }


# --- canonical_id ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (float("nan"), None),
        (np.nan, None),
        (np.float64("nan"), None),
        (17850, "17850"),
        (17850.0, "17850"),
        (np.float64(17850.0), "17850"),
        (np.int64(71053), "71053"),
        ("17850", "17850"),
        ("85123A", "85123A"),
        ("85123a", "85123a"),
        ("C536379", "C536379"),
        (0, "0"),
        (0.0, "0"),
        (-0.0, "0"),
        (1.5, "1.5"),
        (1e15, "1000000000000000"),
        # Text is kept exactly: only a numeric cell can be turned into an integer.
        ("17850.0", "17850.0"),
        ("", ""),
        (" 17 ", " 17 "),
    ],
)
def test_canonical_id(value: object, expected: str | None) -> None:
    assert canonical_id(value) == expected


def test_canonical_id_never_produces_a_point_zero() -> None:
    for number in (12346.0, 18287.0, 1.0):
        assert not canonical_id(number).endswith(".0")


# --- value lists, in the order of PDF section 5.4.3 ---------------------------------


def test_gender_order() -> None:
    assert GENDERS == ("Male", "Female")


def test_age_group_order_uses_ascii_hyphens() -> None:
    assert AGE_GROUPS == ("0-17", "18-25", "26-35", "36-45", "46-50", "51-55", "55+")
    assert all(group.isascii() for group in AGE_GROUPS)


def test_occupation_order() -> None:
    assert OCCUPATIONS == (
        "Student",
        "Engineer",
        "Teacher",
        "Executive",
        "Clerk",
        "Healthcare",
        "Retail",
        "Homemaker",
        "Artist",
        "Other",
    )


def test_city_category_order() -> None:
    assert CITY_CATEGORIES == ("A", "B", "C")


def test_stay_years_order_is_text() -> None:
    assert STAY_YEARS == ("0", "1", "2", "3", "4+")


def test_marital_status_order() -> None:
    assert MARITAL_STATUSES == ("Single", "Married")


def test_product_category_order() -> None:
    assert PRODUCT_CATEGORIES == (
        "Home Decor",
        "Kitchen",
        "Stationery",
        "Toys",
        "Seasonal Gifts",
        "Bags",
        "Jewelry",
        "Garden",
        "Party",
        "Other",
    )


def test_supplier_order() -> None:
    assert SUPPLIERS == (
        ("SUP01", "Northwind Supplies"),
        ("SUP02", "Atlantic Gifts Ltd"),
        ("SUP03", "Midlands Wholesale"),
        ("SUP04", "Celtic Craft Co"),
        ("SUP05", "EuroPack Partners"),
    )


# --- time_parts --------------------------------------------------------------------


def test_time_parts_has_exactly_the_dim_time_columns_in_order() -> None:
    assert list(time_parts(datetime(2010, 12, 1, 8, 26))) == DIM_TIME_COLUMNS


def test_time_parts_full_row() -> None:
    assert time_parts(datetime(2010, 12, 1, 8, 26)) == {
        "time_key": 201012010826,
        "full_datetime": datetime(2010, 12, 1, 8, 26),
        "full_date": date(2010, 12, 1),
        "hour_of_day": 8,
        "day_of_month": 1,
        "month_no": 12,
        "month_name": "December",
        "quarter_no": 4,
        "year_no": 2010,
        "day_of_week": 3,
        "day_name": "Wednesday",
        "day_type": "Weekday",
        "season": "Winter",
        "half_year": "H2",
    }


@pytest.mark.parametrize(
    ("moment", "key"),
    [
        (datetime(2011, 1, 2, 3, 4), 201101020304),
        (datetime(2011, 12, 31, 23, 59), 201112312359),
        (datetime(2011, 1, 1, 0, 0), 201101010000),
    ],
)
def test_time_key_is_zero_padded_yyyymmddhhmm(moment: datetime, key: int) -> None:
    assert time_parts(moment)["time_key"] == key


@pytest.mark.parametrize(
    ("moment", "season"),
    [
        (datetime(2011, 1, 1, 0, 0), "Winter"),
        (datetime(2011, 2, 28, 23, 59), "Winter"),
        (datetime(2011, 3, 1, 0, 0), "Spring"),
        (datetime(2011, 4, 15, 12, 0), "Spring"),
        (datetime(2011, 5, 31, 23, 59), "Spring"),
        (datetime(2011, 6, 1, 0, 0), "Summer"),
        (datetime(2011, 7, 15, 12, 0), "Summer"),
        (datetime(2011, 8, 31, 23, 59), "Summer"),
        (datetime(2011, 9, 1, 0, 0), "Fall"),
        (datetime(2011, 10, 15, 12, 0), "Fall"),
        (datetime(2011, 11, 30, 23, 59), "Fall"),
        (datetime(2011, 12, 1, 0, 0), "Winter"),
        (datetime(2011, 12, 31, 23, 59), "Winter"),
    ],
)
def test_season_boundaries(moment: datetime, season: str) -> None:
    assert time_parts(moment)["season"] == season


@pytest.mark.parametrize(
    ("moment", "half"),
    [
        (datetime(2011, 1, 1, 0, 0), "H1"),
        (datetime(2011, 6, 30, 23, 59), "H1"),
        (datetime(2011, 7, 1, 0, 0), "H2"),
        (datetime(2011, 12, 31, 23, 59), "H2"),
    ],
)
def test_half_year_boundaries(moment: datetime, half: str) -> None:
    assert time_parts(moment)["half_year"] == half


@pytest.mark.parametrize(
    ("moment", "quarter"),
    [
        (datetime(2011, 1, 1, 0, 0), 1),
        (datetime(2011, 3, 31, 23, 59), 1),
        (datetime(2011, 4, 1, 0, 0), 2),
        (datetime(2011, 6, 30, 23, 59), 2),
        (datetime(2011, 7, 1, 0, 0), 3),
        (datetime(2011, 9, 30, 23, 59), 3),
        (datetime(2011, 10, 1, 0, 0), 4),
        (datetime(2011, 12, 31, 23, 59), 4),
    ],
)
def test_quarter_boundaries(moment: datetime, quarter: int) -> None:
    assert time_parts(moment)["quarter_no"] == quarter


@pytest.mark.parametrize(
    ("day", "number", "name", "day_type"),
    [
        (3, 1, "Monday", "Weekday"),
        (4, 2, "Tuesday", "Weekday"),
        (5, 3, "Wednesday", "Weekday"),
        (6, 4, "Thursday", "Weekday"),
        (7, 5, "Friday", "Weekday"),
        (8, 6, "Saturday", "Weekend"),
        (9, 7, "Sunday", "Weekend"),
    ],
)
def test_all_seven_weekdays(day: int, number: int, name: str, day_type: str) -> None:
    parts = time_parts(datetime(2011, 1, day, 10, 0))
    assert parts["day_of_week"] == number
    assert parts["day_name"] == name
    assert parts["day_type"] == day_type


@pytest.mark.parametrize(
    ("month", "name"),
    [
        (1, "January"),
        (2, "February"),
        (3, "March"),
        (4, "April"),
        (5, "May"),
        (6, "June"),
        (7, "July"),
        (8, "August"),
        (9, "September"),
        (10, "October"),
        (11, "November"),
        (12, "December"),
    ],
)
def test_all_month_names(month: int, name: str) -> None:
    assert time_parts(datetime(2011, month, 1))["month_name"] == name


def test_full_datetime_follows_the_key_to_the_minute() -> None:
    """A key with minute resolution next to a datetime with seconds would break the
    UNIQUE on full_datetime for two sales in the same minute."""
    parts = time_parts(datetime(2011, 3, 4, 5, 6, 7, 8))
    assert parts["time_key"] == 201103040506
    assert parts["full_datetime"] == datetime(2011, 3, 4, 5, 6)


def test_timestamp_input_gives_plain_python_values() -> None:
    parts = time_parts(pd.Timestamp("2011-03-04 05:06:00"))
    assert type(parts["full_datetime"]) is datetime
    assert type(parts["full_date"]) is date
    assert parts["full_datetime"] == datetime(2011, 3, 4, 5, 6)
