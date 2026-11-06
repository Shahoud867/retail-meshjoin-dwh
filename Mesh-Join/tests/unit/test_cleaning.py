"""Cleaning on small hand-built frames: every test alone, combined, and bad input."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import numpy as np
import openpyxl
import pandas as pd
import pytest

from retail_dw.prepare.cleaning import (
    SOURCE_COLUMNS,
    TEST_CANCELLATION,
    TEST_CUSTOMER,
    TEST_PRICE,
    TEST_QUANTITY,
    DataFileError,
    clean,
    load_raw,
)

NOTHING_REMOVED = {TEST_CANCELLATION: 0, TEST_QUANTITY: 0, TEST_PRICE: 0, TEST_CUSTOMER: 0}


def raw_row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "InvoiceNo": 536365,
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART",
        "Quantity": 6,
        "InvoiceDate": datetime(2010, 12, 1, 8, 26),
        "UnitPrice": 2.55,
        "CustomerID": 17850,
        "Country": "United Kingdom",
    }
    row.update(changes)
    return row


def raw_frame(*rows: dict[str, object]) -> pd.DataFrame:
    """A frame typed as load_raw gives it: object columns holding the cells as stored."""
    return pd.DataFrame(list(rows), columns=list(SOURCE_COLUMNS), dtype=object)


def test_clean_row_is_kept_under_project_column_names() -> None:
    result = clean(raw_frame(raw_row()))
    assert result.before == 1
    assert result.after == 1
    assert result.removed_by_test == NOTHING_REMOVED
    assert list(result.cleaned_df.columns) == [
        "transaction_id",
        "customer_id",
        "product_id",
        "product_name",
        "quantity",
        "unit_price",
        "event_time",
        "country",
    ]
    assert result.cleaned_df.iloc[0].to_dict() == {
        "transaction_id": "536365",
        "customer_id": "17850",
        "product_id": "85123A",
        "product_name": "WHITE HANGING HEART",
        "quantity": 6,
        "unit_price": Decimal("2.55"),
        "event_time": pd.Timestamp("2010-12-01 08:26:00"),
        "country": "United Kingdom",
    }


@pytest.mark.parametrize(
    ("changes", "failed_test"),
    [
        ({"InvoiceNo": "C536379"}, TEST_CANCELLATION),
        ({"Quantity": 0}, TEST_QUANTITY),
        ({"Quantity": -1}, TEST_QUANTITY),
        ({"Quantity": None}, TEST_QUANTITY),
        ({"Quantity": "many"}, TEST_QUANTITY),
        ({"UnitPrice": 0}, TEST_PRICE),
        ({"UnitPrice": 0.0}, TEST_PRICE),
        ({"UnitPrice": -11062.06}, TEST_PRICE),
        ({"UnitPrice": None}, TEST_PRICE),
        ({"UnitPrice": "free"}, TEST_PRICE),
        ({"CustomerID": None}, TEST_CUSTOMER),
        ({"CustomerID": float("nan")}, TEST_CUSTOMER),
        ({"CustomerID": pd.NA}, TEST_CUSTOMER),
        ({"CustomerID": ""}, TEST_CUSTOMER),
        ({"CustomerID": "   "}, TEST_CUSTOMER),
    ],
)
def test_each_rule_alone_removes_the_row_and_counts_only_itself(
    changes: dict[str, object], failed_test: str
) -> None:
    result = clean(raw_frame(raw_row(**changes), raw_row(InvoiceNo=536366)))
    assert result.before == 2
    assert result.after == 1
    assert result.cleaned_df["transaction_id"].tolist() == ["536366"]
    assert result.removed_by_test == {**NOTHING_REMOVED, failed_test: 1}


def test_a_row_failing_several_tests_counts_under_each_but_leaves_once() -> None:
    result = clean(
        raw_frame(
            raw_row(InvoiceNo="C1", Quantity=-1, UnitPrice=0, CustomerID=None),
            raw_row(InvoiceNo="C2", Quantity=-2),
            raw_row(InvoiceNo=3, CustomerID=None),
            raw_row(InvoiceNo=4),
        )
    )
    assert result.before == 4
    assert result.after == 1
    assert result.removed_by_test == {
        TEST_CANCELLATION: 2,
        TEST_QUANTITY: 2,
        TEST_PRICE: 1,
        TEST_CUSTOMER: 2,
    }


@pytest.mark.parametrize("invoice", ["A563185", "c536379", "536C65", 536379])
def test_only_a_leading_capital_c_is_a_cancellation(invoice: object) -> None:
    """The real file has adjustments such as A563185, and they are not cancellations."""
    result = clean(raw_frame(raw_row(InvoiceNo=invoice)))
    assert result.after == 1
    assert result.removed_by_test[TEST_CANCELLATION] == 0


def test_rows_stay_in_file_order_with_a_fresh_index() -> None:
    frame = raw_frame(
        raw_row(InvoiceNo=3),
        raw_row(InvoiceNo=9, Quantity=-1),
        raw_row(InvoiceNo=1),
        raw_row(InvoiceNo=2, InvoiceDate=datetime(2009, 1, 1)),
    )
    cleaned = clean(frame).cleaned_df
    assert cleaned["transaction_id"].tolist() == ["3", "1", "2"]
    assert cleaned.index.tolist() == [0, 1, 2]


def test_exact_duplicates_are_kept() -> None:
    result = clean(raw_frame(raw_row(), raw_row(), raw_row()))
    assert result.after == 3
    assert len(result.cleaned_df) == 3


def test_input_frame_is_not_changed() -> None:
    frame = raw_frame(raw_row(CustomerID=17850.0), raw_row(Quantity=-1))
    before = frame.copy(deep=True)
    clean(frame)
    pd.testing.assert_frame_equal(frame, before)


def test_a_shuffled_or_duplicate_index_on_the_input_does_not_matter() -> None:
    frame = raw_frame(raw_row(InvoiceNo=1), raw_row(InvoiceNo=2, Quantity=0), raw_row(InvoiceNo=3))
    frame.index = [7, 7, 0]
    cleaned = clean(frame).cleaned_df
    assert cleaned["transaction_id"].tolist() == ["1", "3"]


# --- ids and number types ----------------------------------------------------------


@pytest.mark.parametrize("cell", [17850, 17850.0, np.float64(17850.0), np.int64(17850), "17850"])
def test_every_form_of_a_customer_id_gives_the_same_text(cell: object) -> None:
    assert clean(raw_frame(raw_row(CustomerID=cell))).cleaned_df["customer_id"].tolist() == ["17850"]


def test_numeric_columns_from_an_inferred_frame_also_give_text_ids() -> None:
    """A float column with a NaN is the classic way 17850 becomes 17850.0."""
    frame = pd.DataFrame(
        {
            "InvoiceNo": [536365, 536366],
            "StockCode": [71053, 71054],
            "Description": ["A", "B"],
            "Quantity": [1, 2],
            "InvoiceDate": pd.to_datetime(["2011-01-01 10:00", "2011-01-01 10:01"]),
            "UnitPrice": [1.25, 2.5],
            "CustomerID": [17850.0, np.nan],
            "Country": ["France", "Spain"],
        }
    )
    result = clean(frame)
    assert result.cleaned_df["customer_id"].tolist() == ["17850"]
    assert result.cleaned_df["transaction_id"].tolist() == ["536365"]
    assert result.cleaned_df["product_id"].tolist() == ["71053"]
    assert result.removed_by_test[TEST_CUSTOMER] == 1


def test_integer_and_text_stock_codes_become_the_same_id() -> None:
    cleaned = clean(raw_frame(raw_row(StockCode=71053), raw_row(StockCode="71053"))).cleaned_df
    assert cleaned["product_id"].tolist() == ["71053", "71053"]


@pytest.mark.parametrize(
    ("cell", "expected"),
    [
        (2.55, "2.55"),
        (1.25, "1.25"),
        (0.001, "0.001"),
        (8142.75, "8142.75"),
        (0.29, "0.29"),
        (16.95, "16.95"),
        (1, "1.0"),
        (np.float64(2.55), "2.55"),
        ("2.55", "2.55"),
    ],
)
def test_price_is_the_decimal_of_the_float_repr(cell: object, expected: str) -> None:
    price = clean(raw_frame(raw_row(UnitPrice=cell))).cleaned_df["unit_price"].iloc[0]
    assert type(price) is Decimal
    assert str(price) == expected


def test_quantity_is_a_whole_number_column() -> None:
    cleaned = clean(raw_frame(raw_row(Quantity=6), raw_row(Quantity=80995.0))).cleaned_df
    assert cleaned["quantity"].dtype == "int64"
    assert cleaned["quantity"].tolist() == [6, 80995]


def test_description_and_country_can_be_missing_or_not_text() -> None:
    cleaned = clean(
        raw_frame(
            raw_row(Description=None, Country=None),
            raw_row(Description=20713, Country=float("nan")),
        )
    ).cleaned_df
    assert cleaned["product_name"].tolist() == [None, "20713"]
    assert cleaned["country"].tolist() == [None, None]


def test_description_keeps_its_whitespace_for_the_master_data_rule() -> None:
    cleaned = clean(raw_frame(raw_row(Description="  STARS GIFT TAPE "))).cleaned_df
    assert cleaned["product_name"].tolist() == ["  STARS GIFT TAPE "]


def test_empty_file_gives_empty_result() -> None:
    result = clean(raw_frame())
    assert (result.before, result.after) == (0, 0)
    assert result.removed_by_test == NOTHING_REMOVED
    assert result.cleaned_df.empty


# --- errors ------------------------------------------------------------------------


@pytest.mark.parametrize("column", SOURCE_COLUMNS)
def test_missing_column_error_names_it(column: str) -> None:
    frame = raw_frame(raw_row()).drop(columns=[column])
    with pytest.raises(DataFileError, match=column):
        clean(frame)


def test_all_missing_columns_are_listed() -> None:
    frame = raw_frame(raw_row()).drop(columns=["Quantity", "Country"])
    with pytest.raises(DataFileError, match="Quantity, Country"):
        clean(frame)


def test_extra_columns_are_ignored() -> None:
    frame = raw_frame(raw_row()).assign(Notes="x")
    assert "Notes" not in clean(frame).cleaned_df.columns


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"InvoiceNo": None}, "no transaction id"),
        ({"InvoiceNo": " "}, "no transaction id"),
        ({"StockCode": None}, "no product id"),
        ({"InvoiceDate": None}, "no usable event time"),
        ({"InvoiceDate": "not a date"}, "no usable event time"),
        ({"InvoiceDate": datetime(2011, 1, 1, 10, 0, 30)}, "with seconds"),
        ({"Quantity": 1.5}, "not a whole number"),
        ({"UnitPrice": 1.2345}, "more than 3 decimals"),
    ],
)
def test_a_kept_row_that_cannot_be_stored_is_refused_with_its_row_number(
    changes: dict[str, object], message: str
) -> None:
    """Cleaning is a closed list, so these rows may not be dropped quietly."""
    frame = raw_frame(raw_row(), raw_row(), raw_row(**changes))
    with pytest.raises(DataFileError, match=message) as error:
        clean(frame)
    assert "worksheet row 4" in str(error.value)


def test_a_bad_value_on_a_removed_row_is_not_an_error() -> None:
    frame = raw_frame(raw_row(Quantity=-1, InvoiceDate=None, StockCode=None, UnitPrice=1.2345))
    assert clean(frame).after == 0


# --- load_raw ----------------------------------------------------------------------


def write_workbook(path: Path, rows: list[list[object]]) -> None:
    workbook = openpyxl.Workbook()
    for row in rows:
        workbook.active.append(row)
    workbook.save(path)


def test_load_raw_keeps_cells_as_stored(tmp_path: Path) -> None:
    path = tmp_path / "raw.xlsx"
    write_workbook(
        path,
        [
            list(SOURCE_COLUMNS),
            [536365, "85123A", "A", 6, datetime(2010, 12, 1, 8, 26), 2.55, 17850, "France"],
            ["C536379", 71053, None, -1, datetime(2010, 12, 1, 9, 41), 27.5, None, "Spain"],
        ],
    )
    raw = load_raw(path)
    assert raw.dtypes.tolist() == [object] * len(SOURCE_COLUMNS)
    assert raw["InvoiceNo"].tolist() == [536365, "C536379"]
    assert raw["CustomerID"].iloc[0] == 17850
    assert type(raw["CustomerID"].iloc[0]) is int

    result = clean(raw)
    assert (result.before, result.after) == (2, 1)
    assert result.cleaned_df["customer_id"].tolist() == ["17850"]


def test_load_raw_missing_file_is_a_clear_error(tmp_path: Path) -> None:
    with pytest.raises(DataFileError, match="no_such.xlsx"):
        load_raw(tmp_path / "no_such.xlsx")


@pytest.mark.parametrize("content", [b"not a workbook", b"PK\x03\x04broken zip"])
def test_load_raw_unreadable_file_is_a_clear_error(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "broken.xlsx"
    path.write_bytes(content)
    with pytest.raises(DataFileError, match="broken.xlsx"):
        load_raw(path)
