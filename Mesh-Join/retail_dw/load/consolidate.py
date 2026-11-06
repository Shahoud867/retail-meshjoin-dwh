"""Data consolidation (PDF section 6.2): one joined transaction becomes one DW row.

SALE exists in neither the stream nor the Master Data, so this is the only place in
the project that multiplies quantity by price. Everything later carries that value
as it is. Store attributes are also applied here: the PDF keeps Store_list in memory
and outside MESHJOIN, so no store tuple ever passes through the join.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Context, Decimal, Inexact, InvalidOperation, Overflow
from typing import Any

from retail_dw.meshjoin.model import Transaction
from retail_dw.prepare.rules import STORE_LIST, store_id_for, time_parts

_THREE_PLACES = Decimal("0.001")

# Inexact is trapped because a price with more decimals than the warehouse can hold
# must stop the load. Rounding it quietly would make the revenue totals wrong, and a
# small enough revenue would round to zero.
_EXACT = Context(prec=28, traps=[Inexact, InvalidOperation, Overflow])


@dataclass(frozen=True, slots=True)
class EnrichedRow:
    """Everything needed to load one fact row and any dimension row it introduces.

    Natural ids only: surrogate keys are looked up at load time, because a row can
    wait in memory or in the cache for a long time before it reaches the warehouse.
    """

    transaction_id: str
    customer_id: str
    gender: str
    age_group: str
    occupation: str
    city_category: str
    stay_in_current_city_years: str
    marital_status: str
    product_id: str
    product_name: str
    product_category: str
    supplier_id: str
    supplier_name: str
    store_id: int
    store_name: str
    city: str
    event_time: datetime
    # Keyed by the dim_time column names.
    time: Mapping[str, Any] = field(hash=False)
    quantity: int
    revenue: Decimal
    sales_mode: str
    processing_mode: str
    # Periodic and Shed rows only, so that a load can mark the staging row as done.
    staging_id: int | None = None


def _exact_revenue(quantity: int, unit_price: Decimal) -> Decimal:
    """SALE = QUANTITY x PRICE, to the three places of fact_sales.revenue."""
    return _EXACT.multiply(Decimal(quantity), unit_price).quantize(
        _THREE_PLACES, context=_EXACT
    )


def _store_and_time(store_id: int, event_time: datetime) -> dict[str, Any]:
    """The attributes that come from memory and from the clock, not from the join."""
    store_name, city = STORE_LIST[store_id]
    return {"store_name": store_name, "city": city, "time": time_parts(event_time)}


def consolidate(tx: Transaction) -> EnrichedRow:
    """Build the DW row of a transaction that finished both MESHJOIN passes.

    Raises ValueError for an unmatched transaction: it belongs in etl_reject, and a
    row with a missing dimension attribute would be wrong in every total that uses it.
    """
    if not tx.is_enriched:
        raise ValueError(f"Cannot consolidate an unmatched transaction: {tx!r}")
    (
        customer_id,
        gender,
        age_group,
        occupation,
        city_category,
        stay_in_current_city_years,
        marital_status,
    ) = tx.customer
    product_id, product_name, product_category, supplier_id, supplier_name = tx.product
    store_id = store_id_for(tx.transaction_id, tx.sales_mode)
    return EnrichedRow(
        transaction_id=tx.transaction_id,
        customer_id=customer_id,
        gender=gender,
        age_group=age_group,
        occupation=occupation,
        city_category=city_category,
        stay_in_current_city_years=stay_in_current_city_years,
        marital_status=marital_status,
        product_id=product_id,
        product_name=product_name,
        product_category=product_category,
        supplier_id=supplier_id,
        supplier_name=supplier_name,
        store_id=store_id,
        event_time=tx.event_time,
        quantity=tx.quantity,
        revenue=_exact_revenue(tx.quantity, tx.unit_price),
        sales_mode=tx.sales_mode,
        processing_mode=tx.processing_mode,
        staging_id=tx.staging_id,
        **_store_and_time(store_id, tx.event_time),
    )


def rebuild_row(stored: Mapping[str, Any]) -> EnrichedRow:
    """Rebuild a row from columns kept on disk, given by EnrichedRow field name.

    The stored revenue is used as it is. Store attributes and time parts are not
    stored, so they are applied again by the same code that applied them first.
    """
    return EnrichedRow(**stored, **_store_and_time(stored["store_id"], stored["event_time"]))
