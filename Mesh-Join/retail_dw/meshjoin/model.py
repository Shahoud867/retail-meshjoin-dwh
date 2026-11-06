"""The two records MESHJOIN works with: one transaction and one chunk of them."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

SALES_MODE_ONLINE = "Online"
SALES_MODE_PERIODIC = "Periodic"

# The route a transaction took to the warehouse. The PDF names processing_mode in
# section 6.2 without defining it; the ReadMe documents this reading of it.
PROCESSING_LIVE = "Live"
PROCESSING_PERIODIC = "Periodic"
PROCESSING_SHED = "Shed"


class Transaction:
    """One tuple of the stream S: one product in one shopping cart.

    Slots keep the object small, because up to 5,500 of them are in memory at once.
    Hash table H and the chunk hold references to the same object, so enriching it
    during a pass is visible everywhere and no second copy ever exists.
    """

    __slots__ = (
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "event_time",
        "sales_mode",
        "processing_mode",
        "staging_id",
        "enqueued_at",
        "chunk_id",
        "customer",
        "product",
    )

    def __init__(
        self,
        transaction_id: str,
        customer_id: str,
        product_id: str,
        quantity: int,
        unit_price: Decimal,
        event_time: datetime,
        sales_mode: str,
        processing_mode: str,
        staging_id: int | None = None,
        enqueued_at: float = 0.0,
    ) -> None:
        self.transaction_id = transaction_id
        self.customer_id = customer_id
        self.product_id = product_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.event_time = event_time
        self.sales_mode = sales_mode
        self.processing_mode = processing_mode
        # Periodic and shed rows only: links the row back to the staging table so a
        # load can mark it done and a resumed run can tell it was finished.
        self.staging_id = staging_id
        self.enqueued_at = enqueued_at
        self.chunk_id: int | None = None
        # Filled in by the customer pass and the product pass. The whole Master Data
        # tuple is kept, in the column order of its table.
        self.customer: tuple | None = None
        self.product: tuple | None = None

    @property
    def is_enriched(self) -> bool:
        """True once both passes have found this transaction's Master Data tuple."""
        return self.customer is not None and self.product is not None

    def __repr__(self) -> str:
        return (
            f"Transaction({self.transaction_id!r}, customer={self.customer_id!r}, "
            f"product={self.product_id!r}, {self.processing_mode})"
        )


class Chunk:
    """Up to w transactions that entered H together in one iteration.

    A chunk counts the partitions it has been compared with, per Master Data table.
    It may leave only when both counts cover the whole of their table.
    """

    __slots__ = ("id", "transactions", "customer_visits", "product_visits")

    def __init__(self, chunk_id: int, transactions: list[Transaction]) -> None:
        self.id = chunk_id
        self.transactions = transactions
        self.customer_visits = 0
        self.product_visits = 0

    def __len__(self) -> int:
        return len(self.transactions)

    def __repr__(self) -> str:
        return (
            f"Chunk(id={self.id}, size={len(self.transactions)}, "
            f"customer_visits={self.customer_visits}, "
            f"product_visits={self.product_visits})"
        )
