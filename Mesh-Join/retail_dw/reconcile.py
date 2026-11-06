"""Row accounting: every cleaned transaction must be somewhere, and exactly once."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from retail_dw.db import Database
from retail_dw.threads.online_thread import count_online_rows


@dataclass(frozen=True)
class RowAccount:
    """Where the cleaned transactions are. A row in `in_flight` was in memory when a run
    died; --resume returns it to staging."""

    cleaned: int
    fact: int
    live_cache: int
    shed: int
    staging_waiting: int
    in_flight: int
    online_unread: int
    rejected: int
    fact_revenue: Decimal
    fact_quantity: int

    @property
    def accounted(self) -> int:
        return (
            self.fact
            + self.live_cache
            + self.shed
            + self.staging_waiting
            + self.in_flight
            + self.online_unread
            + self.rejected
        )

    @property
    def balanced(self) -> bool:
        return self.accounted == self.cleaned

    @property
    def complete(self) -> bool:
        return self.balanced and self.fact + self.rejected == self.cleaned


def take_account(db: Database, online_path: str, bad_online_lines: int = 0) -> RowAccount:
    """Read the account from the database and the online file, not from run counters, so
    that it can catch a counter or a thread being wrong."""
    online_total = count_online_rows(online_path) - bad_online_lines
    committed_rows = db.query(
        "SELECT online_rows_committed FROM etl_progress WHERE writer = 'meshjoin'"
    )
    committed = committed_rows[0][0] if committed_rows else 0
    fact, revenue, quantity = db.query(
        "SELECT COUNT(*), COALESCE(SUM(revenue), 0), COALESCE(SUM(quantity), 0) FROM fact_sales"
    )[0]
    staging_total, waiting = db.query(
        "SELECT COUNT(*), COALESCE(SUM(released = 0), 0) FROM periodic_sales_data"
    )[0]
    in_flight = db.query(
        "SELECT COUNT(*) FROM periodic_sales_data p "
        "LEFT JOIN shed_table s ON s.staging_id = p.staging_id "
        "WHERE p.released = 1 AND p.loaded = 0 AND s.staging_id IS NULL"
    )[0][0]
    return RowAccount(
        cleaned=online_total + staging_total,
        fact=fact,
        live_cache=db.query("SELECT COUNT(*) FROM live_cache")[0][0],
        shed=db.query("SELECT COUNT(*) FROM shed_table")[0][0],
        staging_waiting=int(waiting),
        in_flight=in_flight,
        online_unread=online_total - committed,
        rejected=db.query("SELECT COUNT(*) FROM etl_reject")[0][0],
        fact_revenue=Decimal(revenue),
        fact_quantity=int(quantity),
    )


def print_account(account: RowAccount, out: Callable[[str], object] = print) -> None:
    out("Row accounting:")
    out(f"  Cleaned transactions:         {account.cleaned:>9,}")
    out(f"  In the fact table:            {account.fact:>9,}")
    out(f"  In the Live cache:            {account.live_cache:>9,}")
    out(f"  On the shed disk:             {account.shed:>9,}")
    out(f"  Waiting in the staging table: {account.staging_waiting:>9,}")
    out(f"  Lost from memory, recoverable:{account.in_flight:>9,}")
    out(f"  Online rows not yet read:     {account.online_unread:>9,}")
    out(f"  Rejected:                     {account.rejected:>9,}")
    if account.balanced:
        out("  Balanced: every transaction is accounted for.")
    else:
        out(f"  MISMATCH: {account.accounted:,} accounted for, {account.cleaned:,} expected.")
    out(f"  Fact quantity {account.fact_quantity:,}, revenue {account.fact_revenue:,}")
    if account.complete:
        out("  Complete: every transaction has been through the whole ETL.")
