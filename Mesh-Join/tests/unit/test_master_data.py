"""Customer and product Master Data rules and their CSV files."""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from retail_dw.prepare.master_data import (
    AGE_GROUPS,
    CITY_CATEGORIES,
    CUSTOMER_COLUMNS,
    CUSTOMER_CSV_NAME,
    GENDERS,
    MARITAL_STATUSES,
    OCCUPATIONS,
    PRODUCT_CATEGORIES,
    PRODUCT_COLUMNS,
    PRODUCT_CSV_NAME,
    STAY_YEARS,
    SUPPLIERS,
    UNKNOWN_PRODUCT_NAME,
    build_customer_master,
    build_product_master,
    write_master_csvs,
)


def independent_stable(text: str, n: int) -> int:
    """The PDF's definition written out again, so the test does not trust the code."""
    return int(hashlib.md5(text.encode("utf-8")).hexdigest()[:8], 16) % n


def customers_frame(*rows: tuple[str, str | None]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["customer_id", "country"], dtype=object)


def products_frame(*rows: tuple[str, str | None]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["product_id", "product_name"], dtype=object)


# --- customers ---------------------------------------------------------------------


def test_customer_tuple_follows_every_rule_in_column_order() -> None:
    [row] = build_customer_master(customers_frame(("17850", "United Kingdom")))
    assert row == (
        "17850",
        GENDERS[independent_stable("17850", 2)],
        AGE_GROUPS[independent_stable("17850age", 7)],
        OCCUPATIONS[independent_stable("17850occ", 10)],
        CITY_CATEGORIES[independent_stable("17850United Kingdom", 3)],
        STAY_YEARS[independent_stable("17850stay", 5)],
        MARITAL_STATUSES[independent_stable("17850mar", 2)],
    )


def test_known_customer_values() -> None:
    """Fixed values, so that a change to any list order or text suffix is noticed. The
    real-data test recomputes all customers independently inside MySQL."""
    [row] = build_customer_master(customers_frame(("12346", "United Kingdom")))
    assert row == ("12346", "Female", "26-35", "Clerk", "C", "0", "Married")


def test_city_category_uses_the_first_row_country_in_file_order() -> None:
    """Customer 12417 is in the real file under both countries, and the two countries
    really do give different categories."""
    belgium = CITY_CATEGORIES[independent_stable("12417Belgium", 3)]
    spain = CITY_CATEGORIES[independent_stable("12417Spain", 3)]
    assert belgium != spain

    [row] = build_customer_master(customers_frame(("12417", "Belgium"), ("12417", "Spain")))
    assert row[4] == belgium

    [row] = build_customer_master(customers_frame(("12417", "Spain"), ("12417", "Belgium")))
    assert row[4] == spain


def test_concatenation_has_no_separator() -> None:
    [row] = build_customer_master(customers_frame(("12417", "Belgium")))
    assert row[4] == CITY_CATEGORIES[independent_stable("12417Belgium", 3)]


def test_one_tuple_per_customer_sorted_by_id() -> None:
    rows = build_customer_master(
        customers_frame(("3", "France"), ("1", "France"), ("3", "France"), ("20", "Spain"))
    )
    assert [row[0] for row in rows] == ["1", "20", "3"]


def test_missing_country_still_gives_a_repeatable_category() -> None:
    [row] = build_customer_master(customers_frame(("12417", None)))
    assert row[4] == CITY_CATEGORIES[independent_stable("12417", 3)]


def test_every_customer_value_comes_from_its_list() -> None:
    frame = customers_frame(*[(str(number), "France") for number in range(12000, 12300)])
    rows = build_customer_master(frame)
    for lists, position in (
        (GENDERS, 1),
        (AGE_GROUPS, 2),
        (OCCUPATIONS, 3),
        (CITY_CATEGORIES, 4),
        (STAY_YEARS, 5),
        (MARITAL_STATUSES, 6),
    ):
        # With 300 customers every value of each list should turn up.
        assert {row[position] for row in rows} == set(lists)


def test_build_is_repeatable() -> None:
    frame = customers_frame(("17850", "France"), ("13047", "Spain"))
    assert build_customer_master(frame) == build_customer_master(frame)


# --- products ----------------------------------------------------------------------


def product_name(*descriptions: str | None) -> str:
    frame = products_frame(*[("85123A", description) for description in descriptions])
    [row] = build_product_master(frame)
    return row[1]


def test_product_tuple_follows_every_rule_in_column_order() -> None:
    [row] = build_product_master(products_frame(("85123A", "HEART")))
    supplier_id, supplier_name = SUPPLIERS[independent_stable("85123A", 5)]
    assert row == (
        "85123A",
        "HEART",
        PRODUCT_CATEGORIES[independent_stable("85123Acat", 10)],
        supplier_id,
        supplier_name,
    )


def test_known_product_values() -> None:
    [row] = build_product_master(products_frame(("10002", "INFLATABLE POLITICAL GLOBE")))
    assert row == (
        "10002",
        "INFLATABLE POLITICAL GLOBE",
        "Home Decor",
        "SUP04",
        "Celtic Craft Co",
    )


def test_longest_description_wins() -> None:
    assert product_name("AB", "ABCD", "ABC") == "ABCD"


def test_edges_are_trimmed_before_comparing_lengths() -> None:
    """Real file, product 21818: without the trim the first name would tie and win."""
    assert (
        product_name("GLITTER CHRISTMAS HEART ", "GLITTER HEART DECORATION")
        == "GLITTER HEART DECORATION"
    )
    assert product_name("AB      ", "ABC") == "ABC"


def test_first_in_file_wins_a_tie() -> None:
    assert product_name("WHITE HEART", "CREAM HEART") == "WHITE HEART"
    assert product_name("CREAM HEART", "WHITE HEART") == "CREAM HEART"


def test_first_in_file_wins_a_tie_that_exists_only_after_trimming() -> None:
    """Real file, product 22986: the second name is longer only by a trailing space."""
    assert product_name("GINGHAM ROSE WRAP", "WRAP GINGHAM ROSE ") == "GINGHAM ROSE WRAP"


def test_stored_name_has_no_edge_whitespace_but_keeps_inner_spaces() -> None:
    assert product_name("  A  B \t") == "A  B"


@pytest.mark.parametrize(
    "descriptions",
    [[None], [""], ["   "], ["\t\n"], [None, ""], ["  ", None]],
)
def test_no_usable_description_gives_unknown(descriptions: list[str | None]) -> None:
    assert product_name(*descriptions) == UNKNOWN_PRODUCT_NAME
    assert UNKNOWN_PRODUCT_NAME == "UNKNOWN"


def test_missing_description_loses_to_a_real_one_in_any_position() -> None:
    assert product_name(None, "A") == "A"
    assert product_name("A", None) == "A"


def test_one_tuple_per_product_sorted_by_id() -> None:
    rows = build_product_master(
        products_frame(("POST", "P"), ("10002", "G"), ("85123A", "H"), ("10002", "G"))
    )
    assert [row[0] for row in rows] == ["10002", "85123A", "POST"]


def test_every_category_and_supplier_turns_up() -> None:
    frame = products_frame(*[(str(number), "X") for number in range(20000, 20300)])
    rows = build_product_master(frame)
    assert {row[2] for row in rows} == set(PRODUCT_CATEGORIES)
    assert {(row[3], row[4]) for row in rows} == set(SUPPLIERS)


# --- CSV files ---------------------------------------------------------------------


def test_csv_files_have_exact_headers_and_round_trip(tmp_path: Path) -> None:
    customers = [("12346", "Female", "26-35", "Clerk", "C", "4+", "Married")]
    products = [
        ("22776", "CAKESTAND, 3 TIER, LOVEHEART", "Toys", "SUP01", "Northwind Supplies"),
        ("22777", 'THE "BEST" MUG', "Bags", "SUP02", "Atlantic Gifts Ltd"),
        ("22778", "CAFÉ SIGN", "Party", "SUP03", "Midlands Wholesale"),
    ]
    folder = tmp_path / "made" / "by" / "the" / "writer"
    write_master_csvs(folder, customers, products)

    with (folder / CUSTOMER_CSV_NAME).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == list(CUSTOMER_COLUMNS)
    assert [tuple(row) for row in rows[1:]] == customers

    with (folder / PRODUCT_CSV_NAME).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == list(PRODUCT_COLUMNS)
    assert [tuple(row) for row in rows[1:]] == products


def test_csv_header_text_is_the_pdf_column_list(tmp_path: Path) -> None:
    write_master_csvs(tmp_path, [], [])
    assert (tmp_path / "customer_master_data.csv").read_bytes() == (
        b"customer_id,gender,age_group,occupation,city_category,"
        b"stay_in_current_city_years,marital_status\n"
    )
    assert (tmp_path / "product_master_data.csv").read_bytes() == (
        b"product_id,product_name,product_category,supplier_id,supplier_name\n"
    )


def test_csv_is_lf_only_utf8_without_bom(tmp_path: Path) -> None:
    write_master_csvs(
        tmp_path,
        [("1", "Male", "0-17", "Other", "A", "0", "Single")],
        [("X", "CAFÉ", "Toys", "SUP01", "Northwind Supplies")],
    )
    for name in (CUSTOMER_CSV_NAME, PRODUCT_CSV_NAME):
        data = (tmp_path / name).read_bytes()
        assert b"\r" not in data
        assert not data.startswith(b"\xef\xbb\xbf")
        assert data.endswith(b"\n")
    assert "CAFÉ".encode("utf-8") in (tmp_path / PRODUCT_CSV_NAME).read_bytes()
