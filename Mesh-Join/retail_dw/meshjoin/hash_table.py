"""Hash table H: the transactions MESHJOIN currently holds in memory (PDF 4.1).

H is a multi-map, because many transactions may share one join attribute. The project
joins with two Master Data tables, so H has one map per join attribute. Both maps point
at the same Transaction objects. The customer pass and the product pass therefore enrich
one object, and H costs two references per transaction and no second copy.

H only ever holds transactions. Its keys are ids taken from the stream, never from Master
Data R, and the only thing that searches H is a Master Data tuple being probed into it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from retail_dw.meshjoin.model import Chunk, Transaction


class HashTable:
    """Hash table H, keyed by customer_id and by product_id."""

    def __init__(self) -> None:
        self.by_customer: dict[str, list[Transaction]] = {}
        self.by_product: dict[str, list[Transaction]] = {}
        self._size = 0
        self._chunk_ids: set[int] = set()

    @property
    def size(self) -> int:
        """How many transactions are in H. Each one counts once, not once per map."""
        return self._size

    def add_chunk(self, chunk: Chunk) -> None:
        """Put every transaction of a chunk into both maps."""
        if chunk.id in self._chunk_ids:
            raise ValueError(f"Chunk {chunk.id} is already in H")
        self._chunk_ids.add(chunk.id)
        for transaction in chunk.transactions:
            transaction.chunk_id = chunk.id
            self.by_customer.setdefault(transaction.customer_id, []).append(transaction)
            self.by_product.setdefault(transaction.product_id, []).append(transaction)
        self._size += len(chunk)

    def remove_chunk(self, chunk: Chunk) -> None:
        """Take a chunk's transactions out of both maps, leaving other chunks' rows.

        Each affected list is rebuilt once. The lists of different keys never overlap,
        so one call touches at most every row of H, however many keys the chunk has.
        """
        if chunk.id not in self._chunk_ids:
            raise ValueError(f"Chunk {chunk.id} is not in H")
        self._chunk_ids.remove(chunk.id)
        customer_ids = {transaction.customer_id for transaction in chunk.transactions}
        product_ids = {transaction.product_id for transaction in chunk.transactions}
        _remove_from_map(self.by_customer, customer_ids, chunk.id)
        _remove_from_map(self.by_product, product_ids, chunk.id)
        self._size -= len(chunk)

    def probe_customer(self, customer_id: str) -> Sequence[Transaction]:
        """All transactions in H with this customer_id. Does not create an entry."""
        return self.by_customer.get(customer_id, ())

    def probe_product(self, product_id: str) -> Sequence[Transaction]:
        """All transactions in H with this product_id. Does not create an entry."""
        return self.by_product.get(product_id, ())


def _remove_from_map(
    table: dict[str, list[Transaction]], keys: Iterable[str], chunk_id: int
) -> None:
    """Drop one chunk's rows under the given keys, and any key left with no rows, so
    that the size of a map follows the rows in memory and not the length of the run."""
    for key in keys:
        kept = [transaction for transaction in table[key] if transaction.chunk_id != chunk_id]
        if kept:
            table[key] = kept
        else:
            del table[key]
