"""Read the raw transactional file and apply the cleaning tests of PDF section 3.

The tests are a closed list. Exact duplicate rows stay, because the PDF does not ask
to remove them and each one is a real line on a shopping cart.
"""

from __future__ import annotations

import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from retail_dw.prepare.rules import canonical_id

SOURCE_COLUMNS = (
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
)

# Keys of CleaningResult.removed_by_test. A row can fail several tests, so these
# overlap and do not add up to before - after.
TEST_CANCELLATION = "cancellation"
TEST_QUANTITY = "quantity_not_positive"
TEST_PRICE = "price_not_positive"
TEST_CUSTOMER = "customer_missing"

_CANCELLATION_PREFIX = "C"
_PRICE_DECIMALS = 3  # The staging column is DECIMAL(10,3).
_FIRST_DATA_ROW = 2  # Worksheet row of the first data row, below the header.


class DataFileError(ValueError):
    """The raw file is missing, unreadable, or holds a value the project cannot store."""


@dataclass(frozen=True)
class CleaningResult:
    """The cleaned rows in file order and the counts the PDF asks to report."""

    cleaned_df: pd.DataFrame
    before: int
    after: int
    removed_by_test: dict[str, int]


def load_raw(path: str | Path) -> pd.DataFrame:
    """Read the worksheet with no type guessing, so cleaning sees the cells as stored."""
    try:
        return pd.read_excel(path, dtype=object)
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        raise DataFileError(f"Cannot read raw data file {path}: {exc}") from exc


def clean(df: pd.DataFrame) -> CleaningResult:
    """Apply the cleaning tests and return rows under the project's column names.

    The result has text ids, int quantity, Decimal unit_price and minute-resolution
    event_time. A row that passes every test but cannot be stored raises DataFileError,
    because dropping it would add a rule the PDF does not have.
    """
    missing = [name for name in SOURCE_COLUMNS if name not in df.columns]
    if missing:
        raise DataFileError(
            f"Raw data file has no column {', '.join(missing)}; "
            f"it needs {', '.join(SOURCE_COLUMNS)}"
        )
    raw = df.reset_index(drop=True)
    transaction_ids = _text_column(raw["InvoiceNo"])
    customer_ids = _text_column(raw["CustomerID"])
    # Anything that is not a number is not greater than zero, so it fails its test.
    quantities = pd.to_numeric(raw["Quantity"], errors="coerce")
    prices = pd.to_numeric(raw["UnitPrice"], errors="coerce")

    failures = {
        TEST_CANCELLATION: _flags(transaction_ids, _is_cancellation),
        TEST_QUANTITY: ~(quantities > 0),
        TEST_PRICE: ~(prices > 0),
        TEST_CUSTOMER: _flags(customer_ids, lambda value: not _is_present(value)),
    }
    failed = pd.Series(False, index=raw.index)
    for flags in failures.values():
        failed |= flags
    kept = ~failed

    # The original positions stay in the index until validation has used them.
    cleaned = pd.DataFrame(
        {
            "transaction_id": transaction_ids[kept],
            "customer_id": customer_ids[kept],
            "product_id": _text_column(raw.loc[kept, "StockCode"]),
            "product_name": _text_column(raw.loc[kept, "Description"]),
            "quantity": quantities[kept],
            "unit_price": _prices_as_decimal(prices[kept]),
            "event_time": pd.to_datetime(raw.loc[kept, "InvoiceDate"], errors="coerce"),
            "country": _text_column(raw.loc[kept, "Country"]),
        }
    )
    _require_storable(cleaned)
    cleaned = cleaned.astype({"quantity": "int64"}).reset_index(drop=True)
    return CleaningResult(
        cleaned_df=cleaned,
        before=len(raw),
        after=len(cleaned),
        removed_by_test={name: int(flags.sum()) for name, flags in failures.items()},
    )


def _text_column(column: pd.Series) -> pd.Series:
    """Convert cells to canonical text, with None for every kind of missing value.

    The explicit object dtype stops pandas from turning None into NaN.
    """
    cells = column.astype(object).where(column.notna(), None).tolist()
    return pd.Series([canonical_id(cell) for cell in cells], index=column.index, dtype=object)


def _flags(column: pd.Series, test: Callable[[Any], bool]) -> pd.Series:
    return pd.Series([test(value) for value in column.tolist()], index=column.index, dtype=bool)


def _is_cancellation(transaction_id: str | None) -> bool:
    return transaction_id is not None and transaction_id.startswith(_CANCELLATION_PREFIX)


def _is_present(value: str | None) -> bool:
    return value is not None and value.strip() != ""


def _prices_as_decimal(prices: pd.Series) -> pd.Series:
    """Convert through repr, which gives the short text a person sees (2.55) and not
    the binary expansion of the float, so revenue totals come out exact."""
    decimals = [Decimal(repr(float(price))) for price in prices.tolist()]
    return pd.Series(decimals, index=prices.index, dtype=object)


def _require_storable(cleaned: pd.DataFrame) -> None:
    """Refuse values the warehouse would reject, or would change without an error."""
    _require(_flags(cleaned["transaction_id"], _is_present), "have no transaction id")
    _require(_flags(cleaned["product_id"], _is_present), "have no product id")
    event_times = cleaned["event_time"]
    _require(event_times.notna(), "have no usable event time")
    # dim_time has one row per minute, so seconds would be merged away.
    _require(event_times == event_times.dt.floor("min"), "have an event time with seconds")
    _require(cleaned["quantity"] % 1 == 0, "have a quantity that is not a whole number")
    # MySQL rounds extra decimals with only a note, which would change revenue quietly.
    _require(
        _flags(cleaned["unit_price"], _has_storable_precision),
        f"have a unit price with more than {_PRICE_DECIMALS} decimals",
    )


def _has_storable_precision(price: Decimal) -> bool:
    return price.is_finite() and price.as_tuple().exponent >= -_PRICE_DECIMALS


def _require(valid: pd.Series, problem: str) -> None:
    if not valid.all():
        failing = valid.index[~valid]
        first_row = int(failing[0]) + _FIRST_DATA_ROW
        raise DataFileError(
            f"{len(failing)} row(s) passed cleaning but {problem}; "
            f"the first is on worksheet row {first_row}"
        )
