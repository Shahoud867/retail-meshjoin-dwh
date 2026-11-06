"""Build, store and export the customer and product Master Data (PDF section 5.4.3).

Every attribute comes from a fixed rule, so an id always receives the same values.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Sequence
from pathlib import Path

import pandas as pd

from retail_dw.db import Database
from retail_dw.prepare.rules import stable

# The etl_progress row that lets a commit lost during preparation be resolved.
PREPARE_WRITER = "prepare"

# Index 0 is the first value; the order is part of the rules and must not change.
GENDERS = ("Male", "Female")
AGE_GROUPS = ("0-17", "18-25", "26-35", "36-45", "46-50", "51-55", "55+")
OCCUPATIONS = (
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
CITY_CATEGORIES = ("A", "B", "C")
STAY_YEARS = ("0", "1", "2", "3", "4+")
MARITAL_STATUSES = ("Single", "Married")
PRODUCT_CATEGORIES = (
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
SUPPLIERS = (
    ("SUP01", "Northwind Supplies"),
    ("SUP02", "Atlantic Gifts Ltd"),
    ("SUP03", "Midlands Wholesale"),
    ("SUP04", "Celtic Craft Co"),
    ("SUP05", "EuroPack Partners"),
)
UNKNOWN_PRODUCT_NAME = "UNKNOWN"

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
CUSTOMER_CSV_NAME = "customer_master_data.csv"
PRODUCT_CSV_NAME = "product_master_data.csv"


def build_customer_master(df: pd.DataFrame) -> list[tuple[str, ...]]:
    """Return one tuple per customer in table column order, sorted by id.

    A customer can appear under several countries. The first cleaned row in file
    order decides, which is why df must still be in file order.
    """
    first_country: dict[str, str | None] = {}
    for customer_id, country in zip(
        df["customer_id"].tolist(), df["country"].tolist(), strict=True
    ):
        first_country.setdefault(customer_id, country)
    return [
        _customer_tuple(customer_id, first_country[customer_id] or "")
        for customer_id in sorted(first_country)
    ]


def build_product_master(df: pd.DataFrame) -> list[tuple[str, ...]]:
    """Return one tuple per product in table column order, sorted by id.

    The name is the longest description after trimming the edges, so stray trailing
    spaces cannot win. The strict comparison leaves the first in file order on a tie,
    which is what df's file order is needed for.
    """
    names: dict[str, str] = {}
    for product_id, description in zip(
        df["product_id"].tolist(), df["product_name"].tolist(), strict=True
    ):
        name = description.strip() if isinstance(description, str) else ""
        if product_id not in names or len(name) > len(names[product_id]):
            names[product_id] = name
    return [
        _product_tuple(product_id, names[product_id] or UNKNOWN_PRODUCT_NAME)
        for product_id in sorted(names)
    ]


def load_master_data(
    db: Database,
    customers: Sequence[Sequence[str]],
    products: Sequence[Sequence[str]],
) -> None:
    """Insert both tables in one transaction, as multi-row parameterised inserts."""

    def insert_both(session: Database) -> None:
        session.executemany(_insert_statement("customer_master_data", CUSTOMER_COLUMNS), customers)
        session.executemany(_insert_statement("product_master_data", PRODUCT_COLUMNS), products)

    db.run_transaction(insert_both, writer=PREPARE_WRITER)


def write_master_csvs(
    directory: str | Path,
    customers: Iterable[Sequence[str]],
    products: Iterable[Sequence[str]],
) -> None:
    """Write the two Master Data files that are part of the submission."""
    folder = Path(directory)
    write_csv(folder / CUSTOMER_CSV_NAME, CUSTOMER_COLUMNS, customers)
    write_csv(folder / PRODUCT_CSV_NAME, PRODUCT_COLUMNS, products)


def write_csv(path: str | Path, header: Sequence[str], rows: Iterable[Sequence]) -> None:
    """Write UTF-8 with LF line ends. newline="" hands line ends to the csv module, so
    the files come out byte-identical on every platform."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def _insert_statement(table: str, columns: Sequence[str]) -> str:
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"


def _customer_tuple(customer_id: str, country: str) -> tuple[str, ...]:
    return (
        customer_id,
        _pick(GENDERS, customer_id),
        _pick(AGE_GROUPS, customer_id + "age"),
        _pick(OCCUPATIONS, customer_id + "occ"),
        _pick(CITY_CATEGORIES, customer_id + country),
        _pick(STAY_YEARS, customer_id + "stay"),
        _pick(MARITAL_STATUSES, customer_id + "mar"),
    )


def _product_tuple(product_id: str, name: str) -> tuple[str, ...]:
    supplier_id, supplier_name = _pick(SUPPLIERS, product_id)
    return (
        product_id,
        name,
        _pick(PRODUCT_CATEGORIES, product_id + "cat"),
        supplier_id,
        supplier_name,
    )


def _pick[T](values: Sequence[T], text: str) -> T:
    return values[stable(text, len(values))]
