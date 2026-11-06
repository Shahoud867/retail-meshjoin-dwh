"""Hash table H: a multi-map over both join attributes, sharing one set of objects."""

from __future__ import annotations

import random
from datetime import datetime
from decimal import Decimal

import pytest

from retail_dw.meshjoin.hash_table import HashTable
from retail_dw.meshjoin.model import PROCESSING_LIVE, SALES_MODE_ONLINE, Chunk, Transaction


def make_transaction(customer_id: str, product_id: str) -> Transaction:
    return Transaction(
        transaction_id="536365",
        customer_id=customer_id,
        product_id=product_id,
        quantity=1,
        unit_price=Decimal("1.250"),
        event_time=datetime(2011, 1, 1, 9, 0),
        sales_mode=SALES_MODE_ONLINE,
        processing_mode=PROCESSING_LIVE,
    )


def test_many_transactions_may_share_one_key_within_a_chunk() -> None:
    first = make_transaction("C1", "P1")
    second = make_transaction("C1", "P2")
    h = HashTable()

    h.add_chunk(Chunk(1, [first, second]))

    assert h.by_customer["C1"] == [first, second]
    assert h.by_product["P1"] == [first]
    assert h.by_product["P2"] == [second]
    assert h.size == 2


def test_keys_are_shared_across_chunks_in_arrival_order() -> None:
    older = make_transaction("C1", "P1")
    newer = make_transaction("C1", "P1")
    other = make_transaction("C2", "P2")
    h = HashTable()

    h.add_chunk(Chunk(1, [older, other]))
    h.add_chunk(Chunk(2, [newer]))

    assert h.by_customer["C1"] == [older, newer]
    assert h.by_product["P1"] == [older, newer]
    assert h.size == 3


def test_remove_chunk_leaves_rows_of_other_chunks_under_shared_keys() -> None:
    older = make_transaction("C1", "P1")
    newer = make_transaction("C1", "P1")
    other = make_transaction("C2", "P1")
    h = HashTable()
    old_chunk = Chunk(1, [older, other])
    new_chunk = Chunk(2, [newer])
    h.add_chunk(old_chunk)
    h.add_chunk(new_chunk)

    h.remove_chunk(old_chunk)

    assert h.by_customer == {"C1": [newer]}
    assert h.by_product == {"P1": [newer]}
    assert h.size == 1


def test_remove_chunk_works_in_any_order() -> None:
    rows = [make_transaction("C1", "P1") for _ in range(3)]
    chunks = [Chunk(index, [row]) for index, row in enumerate(rows, start=1)]
    h = HashTable()
    for chunk in chunks:
        h.add_chunk(chunk)

    h.remove_chunk(chunks[1])

    assert h.by_customer["C1"] == [rows[0], rows[2]]
    assert h.by_product["P1"] == [rows[0], rows[2]]


def test_removing_the_last_row_of_a_key_removes_the_key() -> None:
    """Otherwise the maps would keep growing with every id the stream ever carried."""
    chunk = Chunk(1, [make_transaction("C1", "P1")])
    h = HashTable()
    h.add_chunk(chunk)

    h.remove_chunk(chunk)

    assert h.by_customer == {}
    assert h.by_product == {}
    assert h.size == 0


def test_both_maps_hold_the_same_object_and_not_a_copy() -> None:
    transaction = make_transaction("C1", "P1")
    chunk = Chunk(1, [transaction])
    h = HashTable()

    h.add_chunk(chunk)

    assert h.by_customer["C1"][0] is transaction
    assert h.by_product["P1"][0] is transaction
    assert chunk.transactions[0] is transaction


def test_enriching_through_one_map_is_visible_through_the_other() -> None:
    transaction = make_transaction("C1", "P1")
    h = HashTable()
    h.add_chunk(Chunk(1, [transaction]))
    customer_tuple = ("C1", "Female")
    product_tuple = ("P1", "Teapot")

    h.by_customer["C1"][0].customer = customer_tuple
    h.by_product["P1"][0].product = product_tuple

    assert transaction.customer is customer_tuple
    assert transaction.product is product_tuple
    assert h.by_product["P1"][0].customer is customer_tuple
    assert transaction.is_enriched


def test_size_counts_each_transaction_once_and_not_once_per_map() -> None:
    h = HashTable()
    h.add_chunk(Chunk(1, [make_transaction("C1", "P1"), make_transaction("C1", "P2")]))
    assert h.size == 2


def test_add_chunk_records_which_chunk_a_row_belongs_to() -> None:
    transaction = make_transaction("C1", "P1")
    HashTable().add_chunk(Chunk(7, [transaction]))
    assert transaction.chunk_id == 7


def test_identical_rows_stay_separate() -> None:
    """The cleaned file keeps exact duplicates, and each one is a sale of its own."""
    first = make_transaction("C1", "P1")
    twin = make_transaction("C1", "P1")
    first_chunk = Chunk(1, [first, twin])
    h = HashTable()

    h.add_chunk(first_chunk)

    assert len(h.by_customer["C1"]) == 2
    assert h.by_customer["C1"][0] is not h.by_customer["C1"][1]
    h.remove_chunk(first_chunk)
    assert h.size == 0


def test_keys_are_case_sensitive_like_the_utf8mb4_bin_columns() -> None:
    upper = make_transaction("C1", "15056BL")
    lower = make_transaction("C1", "15056bl")
    h = HashTable()

    h.add_chunk(Chunk(1, [upper, lower]))

    assert h.probe_product("15056BL") == [upper]
    assert h.probe_product("15056bl") == [lower]


def test_probing_returns_every_match_for_both_attributes() -> None:
    rows = [make_transaction("C1", "P1"), make_transaction("C1", "P2")]
    h = HashTable()
    h.add_chunk(Chunk(1, rows))

    assert list(h.probe_customer("C1")) == rows
    assert list(h.probe_product("P2")) == [rows[1]]


def test_probing_an_unknown_key_finds_nothing_and_adds_no_entry() -> None:
    h = HashTable()
    h.add_chunk(Chunk(1, [make_transaction("C1", "P1")]))

    assert list(h.probe_customer("missing")) == []
    assert list(h.probe_product("missing")) == []
    assert set(h.by_customer) == {"C1"}
    assert set(h.by_product) == {"P1"}


def test_an_empty_chunk_changes_nothing() -> None:
    chunk = Chunk(1, [])
    h = HashTable()

    h.add_chunk(chunk)
    assert h.size == 0
    h.remove_chunk(chunk)
    assert h.size == 0


def test_adding_a_chunk_twice_is_refused() -> None:
    chunk = Chunk(1, [make_transaction("C1", "P1")])
    h = HashTable()
    h.add_chunk(chunk)

    with pytest.raises(ValueError, match="already"):
        h.add_chunk(chunk)

    assert h.size == 1
    assert len(h.by_customer["C1"]) == 1


def test_removing_a_chunk_not_in_h_is_refused() -> None:
    chunk = Chunk(1, [make_transaction("C1", "P1")])
    h = HashTable()
    h.add_chunk(chunk)
    h.remove_chunk(chunk)

    with pytest.raises(ValueError, match="not in H"):
        h.remove_chunk(chunk)

    assert h.size == 0


def test_a_long_run_of_random_chunks_keeps_the_maps_exact() -> None:
    """Compares H with a plain model after every step: both maps hold exactly the rows
    of the chunks still in memory, grouped by the right key, with nothing left behind."""
    rng = random.Random(4338)
    h = HashTable()
    resident: list[Chunk] = []
    for chunk_id in range(1, 400):
        rows = [
            make_transaction(f"C{rng.randrange(40)}", f"P{rng.randrange(30)}")
            for _ in range(rng.randint(0, 25))
        ]
        chunk = Chunk(chunk_id, rows)
        h.add_chunk(chunk)
        resident.append(chunk)
        if len(resident) > 9 or rng.random() < 0.2:
            h.remove_chunk(resident.pop(rng.randrange(len(resident))))

        rows_in_memory = [row for chunk in resident for row in chunk.transactions]
        assert h.size == len(rows_in_memory)
        for table, key_of in (
            (h.by_customer, lambda row: row.customer_id),
            (h.by_product, lambda row: row.product_id),
        ):
            grouped: dict[str, list[int]] = {}
            for row in rows_in_memory:
                grouped.setdefault(key_of(row), []).append(id(row))
            actual = {key: sorted(id(row) for row in rows) for key, rows in table.items()}
            assert actual == {key: sorted(ids) for key, ids in grouped.items()}
