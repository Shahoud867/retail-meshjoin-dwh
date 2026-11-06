"""Run data preparation from the raw file to a ready database (PDF section 9, step 2)."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from itertools import chain
from pathlib import Path

from retail_dw.config import PARTITION_SIZE, PERIODIC_GROUPS, ConnectionConfig, Settings
from retail_dw.db import Database
from retail_dw.prepare.cleaning import clean, load_raw
from retail_dw.prepare.master_data import (
    PREPARE_WRITER,
    build_customer_master,
    build_product_master,
    load_master_data,
    write_master_csvs,
)
from retail_dw.prepare.split import (
    build_periodic_groups,
    load_staging,
    mark_sales_mode,
    write_online_file,
)

_MARK_PREPARED = (
    "INSERT INTO etl_progress (writer, prepared) VALUES (%s, 1) "
    "ON DUPLICATE KEY UPDATE prepared = 1"
)


@dataclass(frozen=True)
class PreparationSummary:
    """The counts the banner and the report need."""

    before: int
    after: int
    online_rows: int
    periodic_rows: int
    group_min: int
    group_max: int
    customers: int
    products: int
    customer_partitions: int
    product_partitions: int
    removed_by_test: dict[str, int]


def prepare_all(
    settings: Settings,
    conn_cfg: ConnectionConfig,
    raw_path: str | Path,
    online_path: str | Path,
    out: Callable[[str], object] = print,
) -> PreparationSummary:
    """Clean the raw file, load the Master Data, write the online file, stage the groups.

    The schema must already exist and be empty. The Master Data CSV files go next to
    the online file. prepared=1 is written last: a run that dies part way leaves it
    unset, so --resume refuses that database and a normal start rebuilds it.
    """
    if settings.resume:
        raise ValueError("Preparation rebuilds the data, so it cannot run with --resume")

    result = clean(load_raw(raw_path))
    removed = ", ".join(
        f"{name.replace('_', ' ')} {count:,}" for name, count in result.removed_by_test.items()
    )
    out(f"Transactions before cleaning: {result.before:,}")
    out(f"Transactions after cleaning: {result.after:,}")
    out(f"Failed each test (a transaction can fail several): {removed}")
    out(f"Shopping carts: {result.cleaned_df['transaction_id'].nunique():,}")

    customers = build_customer_master(result.cleaned_df)
    products = build_product_master(result.cleaned_df)
    marked = mark_sales_mode(result.cleaned_df)
    groups = build_periodic_groups(marked)
    group_lengths = [len(group) for group in groups]

    with Database(conn_cfg) as db:
        load_master_data(db, customers, products)
        write_master_csvs(Path(online_path).parent, customers, products)
        online_rows = write_online_file(marked, online_path)
        periodic_rows = load_staging(db, chain.from_iterable(groups))
        # Counted from the tables, as PDF section 4.2 asks.
        customer_count = _count_rows(db, "customer_master_data")
        product_count = _count_rows(db, "product_master_data")
        db.run_transaction(
            lambda session: session.execute(_MARK_PREPARED, (PREPARE_WRITER,)),
            writer=PREPARE_WRITER,
        )

    customer_partitions = math.ceil(customer_count / PARTITION_SIZE)
    product_partitions = math.ceil(product_count / PARTITION_SIZE)
    out(f"Customer Master Data: {customer_count:,} tuples, {customer_partitions} partitions")
    out(f"Product Master Data: {product_count:,} tuples, {product_partitions} partitions")
    out(f"Online file: {online_rows:,} transactions in {online_path}")
    out(
        f"Periodic staging table: {periodic_rows:,} transactions in {PERIODIC_GROUPS} groups, "
        f"smallest {min(group_lengths):,}, largest {max(group_lengths):,}"
    )
    return PreparationSummary(
        before=result.before,
        after=result.after,
        online_rows=online_rows,
        periodic_rows=periodic_rows,
        group_min=min(group_lengths),
        group_max=max(group_lengths),
        customers=customer_count,
        products=product_count,
        customer_partitions=customer_partitions,
        product_partitions=product_partitions,
        removed_by_test=result.removed_by_test,
    )


def _count_rows(db: Database, table: str) -> int:
    return db.query(f"SELECT COUNT(*) FROM {table}")[0][0]
