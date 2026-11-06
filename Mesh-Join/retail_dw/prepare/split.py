"""Divide the cleaned rows into the online file and the periodic staging table."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime
from pathlib import Path

import pandas as pd

from retail_dw.config import PERIODIC_GROUPS
from retail_dw.db import Database
from retail_dw.meshjoin.model import SALES_MODE_ONLINE, SALES_MODE_PERIODIC
from retail_dw.prepare.master_data import PREPARE_WRITER, write_csv
from retail_dw.prepare.rules import sales_mode_for

ONLINE_COLUMNS = (
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "event_time",
    "sales_mode",
)
# Table column order of one row returned by build_periodic_groups.
STAGING_COLUMNS = (*ONLINE_COLUMNS, "release_hour")

_INSERT_STAGING = (
    f"INSERT INTO periodic_sales_data ({', '.join(STAGING_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(STAGING_COLUMNS))})"
)
# Rows per INSERT. A row is about 100 bytes, so a statement stays around 500 KB, well
# under even a small max_allowed_packet.
_STAGING_BATCH_ROWS = 5000


def mark_sales_mode(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with sales_mode added, decided once per cart and not per row."""
    modes = {
        transaction_id: sales_mode_for(transaction_id)
        for transaction_id in df["transaction_id"].unique().tolist()
    }
    return df.assign(sales_mode=[modes[value] for value in df["transaction_id"].tolist()])


def write_online_file(df: pd.DataFrame, path: str | Path) -> int:
    """Write the Online rows in file order and return how many were written.

    A resumed run skips online_rows_committed lines of this file, so the order is
    part of what makes restarts exact.
    """
    online = df[df["sales_mode"] == SALES_MODE_ONLINE]
    write_csv(path, ONLINE_COLUMNS, _online_csv_rows(online))
    return len(online)


def build_periodic_groups(df: pd.DataFrame) -> list[list[tuple]]:
    """Return the Periodic rows as PERIODIC_GROUPS lists of staging rows, by release hour.

    The sort is stable so that rows sharing one minute keep their file order, which
    makes the group boundaries repeatable.
    """
    periodic = df[df["sales_mode"] == SALES_MODE_PERIODIC].sort_values(
        "event_time", kind="stable"
    )
    rows = list(_row_values(periodic))
    groups: list[list[tuple]] = []
    start = 0
    for release_hour, size in enumerate(group_sizes(len(rows))):
        end = start + size
        groups.append([(*row, release_hour) for row in rows[start:end]])
        start = end
    return groups


def group_sizes(total: int, groups: int = PERIODIC_GROUPS) -> list[int]:
    """Split a total as numpy.array_split does: the first groups take the extra rows."""
    size, extra = divmod(total, groups)
    return [size + 1 if index < extra else size for index in range(groups)]


def load_staging(db: Database, rows: Iterable[Sequence]) -> int:
    """Insert staging rows in one transaction and return how many went in.

    Insertion order becomes staging_id order, so the admit query's ORDER BY staging_id
    reads the rows in event_time order. The rows are materialised first because a
    transaction retry needs them a second time, and rows may come from a generator.
    """
    materialised = list(rows)

    def insert_rows(session: Database) -> int:
        return session.executemany(_INSERT_STAGING, materialised, batch_size=_STAGING_BATCH_ROWS)

    return db.run_transaction(insert_rows, writer=PREPARE_WRITER)


def _row_values(df: pd.DataFrame) -> Iterator[tuple]:
    """Yield rows of plain Python values, which is what the MySQL driver can convert."""
    return zip(
        df["transaction_id"].tolist(),
        df["customer_id"].tolist(),
        df["product_id"].tolist(),
        df["quantity"].tolist(),
        df["unit_price"].tolist(),
        _python_datetimes(df["event_time"]),
        df["sales_mode"].tolist(),
        strict=True,
    )


def _python_datetimes(column: pd.Series) -> list[datetime]:
    return [stamp.to_pydatetime() for stamp in pd.to_datetime(column)]


def _online_csv_rows(online: pd.DataFrame) -> Iterator[tuple]:
    for (
        transaction_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        event_time,
        sales_mode,
    ) in _row_values(online):
        yield (
            transaction_id,
            customer_id,
            product_id,
            quantity,
            format(unit_price, "f"),
            event_time.isoformat(" ", "seconds"),
            sales_mode,
        )
