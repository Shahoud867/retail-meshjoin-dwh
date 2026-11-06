"""The data rules, rewritten from the assignment text for checking purposes only.

Nothing here imports retail_dw.prepare. A bug in the real preparation code can only be
caught by something that did not inherit it, so this module is deliberately a second,
separate reading of the PDF: vectorised where prepare is row by row, and holding money
as integer thousandths where prepare uses Decimal.

Money is an int64 count of thousandths ("mills") because prices have at most three
decimals. Every total is then an exact integer sum, and pandas never touches a float.
"""

from __future__ import annotations

import hashlib
from datetime import date
from functools import cache
from pathlib import Path

import pandas as pd

RAW_FILE = Path(__file__).resolve().parents[3] / "Data" / "raw" / "Online Retail.xlsx"

_GENDERS = ("Male", "Female")
_AGE_GROUPS = ("0-17", "18-25", "26-35", "36-45", "46-50", "51-55", "55+")
_OCCUPATIONS = (
    "Student", "Engineer", "Teacher", "Executive", "Clerk",
    "Healthcare", "Retail", "Homemaker", "Artist", "Other",
)
_CITY_CATEGORIES = ("A", "B", "C")
_STAY_YEARS = ("0", "1", "2", "3", "4+")
_MARITAL = ("Single", "Married")
_CATEGORIES = (
    "Home Decor", "Kitchen", "Stationery", "Toys", "Seasonal Gifts",
    "Bags", "Jewelry", "Garden", "Party", "Other",
)
_SUPPLIERS = (
    ("SUP01", "Northwind Supplies"),
    ("SUP02", "Atlantic Gifts Ltd"),
    ("SUP03", "Midlands Wholesale"),
    ("SUP04", "Celtic Craft Co"),
    ("SUP05", "EuroPack Partners"),
)
_STORES = {
    1: ("Online", "Web"),
    2: ("London Bridge", "London"),
    3: ("Manchester Arndale", "Manchester"),
    4: ("Birmingham Bullring", "Birmingham"),
    5: ("Edinburgh Princes", "Edinburgh"),
}
_SEASON_OF_MONTH = {
    12: "Winter", 1: "Winter", 2: "Winter",
    3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer",
    9: "Fall", 10: "Fall", 11: "Fall",
}

_MILLS_PER_UNIT = 1000
_FLOAT_NOISE = 1e-6


def md5_prefix(text: str) -> int:
    """First eight hexadecimal digits of the MD5 of the UTF-8 text, as a number."""
    return int(hashlib.md5(text.encode("utf-8")).hexdigest()[:8], 16)


def is_online_cart(invoice: str) -> bool:
    """PDF 5.4.1: remainders 0 to 6 are the website, 7 to 9 are a physical store."""
    return md5_prefix(invoice) % 10 <= 6


def store_of_cart(invoice: str) -> int:
    """PDF 5.4.2: online carts use store 1, others one of the four physical stores."""
    return 1 if is_online_cart(invoice) else 2 + md5_prefix(invoice) % 4


def id_as_text(value: object) -> str | None:
    """The spreadsheet stores some ids as floats, so 17850 arrives as 17850.0, and the
    MD5 rules hash the text. Written once here so the two forms can never disagree."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _attribute_columns(ids: pd.Series, rules: dict[str, tuple[str, tuple]]) -> pd.DataFrame:
    """Evaluate `{column: (salt, options)}` for each distinct id. An id appears many
    times in the file, and hashing each distinct id once is what keeps this fast."""
    return pd.DataFrame(
        {
            column: [options[md5_prefix(key + salt) % len(options)] for key in ids]
            for column, (salt, options) in rules.items()
        },
        index=ids,
        dtype=object,
    )


def _customer_attributes(clean: pd.DataFrame) -> pd.DataFrame:
    """One row per customer. City depends on the country of the first cleaned row."""
    first_rows = clean.drop_duplicates("customer_id", keep="first")
    customers = first_rows["customer_id"]
    attributes = _attribute_columns(
        customers,
        {
            "gender": ("", _GENDERS),
            "age_group": ("age", _AGE_GROUPS),
            "occupation": ("occ", _OCCUPATIONS),
            "stay_in_current_city_years": ("stay", _STAY_YEARS),
            "marital_status": ("mar", _MARITAL),
        },
    )
    attributes["city_category"] = [
        _CITY_CATEGORIES[md5_prefix(customer + country) % len(_CITY_CATEGORIES)]
        for customer, country in zip(customers, first_rows["country"], strict=True)
    ]
    return attributes


def _product_attributes(clean: pd.DataFrame) -> pd.DataFrame:
    """One row per product. A stable sort by length keeps file order among equally long
    descriptions, which is the tie break the PDF leaves open."""
    named = clean[["product_id"]].assign(
        product_name=[
            "" if pd.isna(text) else str(text).strip() for text in clean["description"]
        ]
    )
    named["name_length"] = named["product_name"].str.len()
    longest = named.sort_values("name_length", ascending=False, kind="stable").drop_duplicates(
        "product_id", keep="first"
    )
    products = longest["product_id"]
    attributes = _attribute_columns(products, {"product_category": ("cat", _CATEGORIES)})
    attributes["product_name"] = [name or "UNKNOWN" for name in longest["product_name"]]
    suppliers = [_SUPPLIERS[md5_prefix(product) % len(_SUPPLIERS)] for product in products]
    attributes["supplier_id"] = [supplier_id for supplier_id, _ in suppliers]
    attributes["supplier_name"] = [supplier_name for _, supplier_name in suppliers]
    return attributes


def _add_time_columns(frame: pd.DataFrame) -> None:
    moment = frame["event_time"].dt
    # The parts are int32, and YYYYMMDDHHMM does not fit: without this cast the sum wraps
    # around silently and still looks like a set of unique keys.
    frame["time_key"] = (
        moment.year.astype("int64") * 100_000_000
        + moment.month * 1_000_000
        + moment.day * 10_000
        + moment.hour * 100
        + moment.minute
    )
    frame["full_date"] = pd.Series(
        [date(stamp.year, stamp.month, stamp.day) for stamp in frame["event_time"]],
        index=frame.index,
        dtype=object,
    )
    frame["hour_of_day"] = moment.hour
    frame["day_of_month"] = moment.day
    frame["month_no"] = moment.month
    frame["month_name"] = moment.month_name()
    frame["quarter_no"] = moment.quarter
    frame["year_no"] = moment.year
    frame["day_of_week"] = moment.day_of_week + 1
    frame["day_name"] = moment.day_name()
    frame["day_type"] = moment.day_of_week.map(lambda day: "Weekend" if day >= 5 else "Weekday")
    frame["season"] = moment.month.map(_SEASON_OF_MONTH)
    frame["half_year"] = moment.month.map(lambda month: "H1" if month <= 6 else "H2")


def read_clean_rows(raw_path: Path = RAW_FILE) -> pd.DataFrame:
    """Read the workbook and apply the four cleaning tests of PDF section 3."""
    raw = pd.read_excel(raw_path, dtype=object)
    frame = pd.DataFrame(
        {
            "transaction_id": raw["InvoiceNo"].map(id_as_text).astype(object),
            "customer_id": raw["CustomerID"].map(id_as_text).astype(object),
            "product_id": raw["StockCode"].map(id_as_text).astype(object),
            "description": raw["Description"].astype(object),
            "quantity": raw["Quantity"].astype("int64"),
            "unit_price": raw["UnitPrice"].astype("float64"),
            "event_time": pd.to_datetime(raw["InvoiceDate"]),
            "country": raw["Country"].astype(object),
        }
    )
    keep = (
        (frame["quantity"] > 0)
        & (frame["unit_price"] > 0)
        & frame["customer_id"].notna()
        & ~frame["transaction_id"].map(lambda invoice: invoice.startswith("C"))
    )
    return frame[keep].reset_index(drop=True)


def _price_in_mills(price: pd.Series) -> pd.Series:
    """Convert to whole thousandths, refusing a price that has a fourth decimal. The
    file has none, and silently rounding one would make every total here look exact
    while actually being wrong."""
    thousandths = price * _MILLS_PER_UNIT
    mills = thousandths.round()
    if ((thousandths - mills).abs() > _FLOAT_NOISE).any():
        raise ValueError("A unit price has more than three decimals")
    return mills.astype("int64")


@cache
def enriched_rows(raw_path: Path = RAW_FILE) -> pd.DataFrame:
    """Every cleaned transaction with all its dimension attributes, in file order.

    Reading the workbook takes about a minute, so one process does it once. Callers
    must treat the result as read-only.
    """
    clean = read_clean_rows(raw_path)
    frame = clean[["transaction_id", "customer_id", "product_id", "quantity", "event_time"]].copy()
    frame["revenue_mills"] = clean["quantity"] * _price_in_mills(clean["unit_price"])

    online = frame["transaction_id"].map(is_online_cart)
    frame["sales_mode"] = online.map({True: "Online", False: "Periodic"})
    frame["processing_mode"] = online.map({True: "Live", False: "Periodic"})
    frame["store_id"] = frame["transaction_id"].map(store_of_cart)
    frame["store_name"] = frame["store_id"].map(lambda store: _STORES[store][0])
    frame["city"] = frame["store_id"].map(lambda store: _STORES[store][1])

    frame = frame.join(_customer_attributes(clean), on="customer_id")
    frame = frame.join(_product_attributes(clean), on="product_id")
    _add_time_columns(frame)
    return frame
