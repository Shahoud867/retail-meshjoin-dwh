"""MESHJOIN, driven through its public interface with Master Data held in lists.

The sources are tuned to the real tables: 4,338 customers make Nc = 9 partitions and 3,665
products make Np = 8, and because 9 and 8 share no factor, the two cursors go through 72
distinct phases. These tests need no database, which is the point of injecting the source.
"""

from __future__ import annotations

import random
import subprocess
import sys
from collections import Counter
from datetime import datetime
from decimal import Decimal
from itertools import product as cartesian
from pathlib import Path

import pytest

from retail_dw.meshjoin.chunk_queue import ChunkQueue
from retail_dw.meshjoin.disk_buffer import (
    DiskBuffer,
    EmptyMasterDataError,
    ListPartitionSource,
    PartitionSource,
)
from retail_dw.meshjoin.engine import MeshJoinEngine
from retail_dw.meshjoin.model import (
    PROCESSING_LIVE,
    PROCESSING_PERIODIC,
    PROCESSING_SHED,
    SALES_MODE_ONLINE,
    SALES_MODE_PERIODIC,
    Chunk,
    Transaction,
)
from retail_dw.meshjoin.stream_buffer import LANE_PERIODIC, LANE_SHED, StreamBuffer

SIZE = 500
NC = 9
NP = 8
EVERY_ENTRY_PHASE = list(cartesian(range(NC), range(NP)))


def customer_rows(count: int) -> list[tuple]:
    return [
        (f"C{i:05d}", "Female", "18-25", "Student", "A", "1", "Single") for i in range(count)
    ]


def product_rows(count: int) -> list[tuple]:
    return [
        (f"P{i:05d}", f"Product {i}", "Toys", "SUP01", "Northwind Supplies")
        for i in range(count)
    ]


CUSTOMERS = customer_rows(4338)
PRODUCTS = product_rows(3665)


def make_transaction(
    sequence: int,
    customer_id: str,
    product_id: str,
    processing_mode: str = PROCESSING_LIVE,
) -> Transaction:
    return Transaction(
        transaction_id=str(sequence),
        customer_id=customer_id,
        product_id=product_id,
        quantity=sequence + 1,
        unit_price=Decimal("2.550"),
        event_time=datetime(2011, 3, 1, 8, 30),
        sales_mode=SALES_MODE_ONLINE if processing_mode == PROCESSING_LIVE else SALES_MODE_PERIODIC,
        processing_mode=processing_mode,
    )


def stream_covering_all_master_data() -> list[Transaction]:
    """One row per customer, cycling over the products, so every tuple of R has a match."""
    return [
        make_transaction(i, CUSTOMERS[i][0], PRODUCTS[i % len(PRODUCTS)][0])
        for i in range(len(CUSTOMERS))
    ]


def build_engine(
    customers: list[tuple] = CUSTOMERS,
    products: list[tuple] = PRODUCTS,
    customer_phase: int = 0,
    product_phase: int = 0,
    limit: int = 1000,
    w: int = 500,
) -> MeshJoinEngine:
    """An engine whose cursors have already read customer_phase and product_phase
    partitions, as they would have if the stream had been running for a while."""
    customer_source = ListPartitionSource(customers, SIZE)
    product_source = ListPartitionSource(products, SIZE)
    for _ in range(customer_phase):
        customer_source.next_partition()
    for _ in range(product_phase):
        product_source.next_partition()
    return MeshJoinEngine(
        StreamBuffer(limit=limit, online_reserve=0), customer_source, product_source, w=w
    )


def run_to_completion(engine: MeshJoinEngine, stream: list[Transaction]) -> list[Chunk]:
    """Feed the stream as fast as the Stream buffer accepts it, until H is empty."""
    finished: list[Chunk] = []
    position = 0
    while position < len(stream) or engine.has_work():
        position += engine.stream_buffer.put_online(stream[position:], timeout=0)
        finished.extend(engine.run_iteration())
    return finished


def rows_of(chunks: list[Chunk]) -> list[Transaction]:
    return [transaction for chunk in chunks for transaction in chunk.transactions]


def assert_fully_enriched(transaction: Transaction) -> None:
    """Enriched with the right tuple, not merely with some tuple."""
    assert transaction.customer is not None, transaction
    assert transaction.product is not None, transaction
    assert transaction.customer[0] == transaction.customer_id
    assert transaction.product[0] == transaction.product_id


def partition_seen_last(phase: int, partitions: int) -> int:
    """A chunk entering when the cursor has read `phase` partitions sees phase + 1 first
    and, one whole cycle later, sees `phase` last. Partition numbers start at 1."""
    return phase if phase else partitions


def tuple_in_partition(rows: list[tuple], partition: int) -> tuple:
    return rows[(partition - 1) * SIZE]


class ReadCountingSource:
    """Offers only what the PartitionSource protocol offers, and counts the reads. An
    engine that tried to search Master Data R in any other way would crash on this."""

    def __init__(self, source: ListPartitionSource) -> None:
        self._source = source
        self.partitions = source.partitions
        self.reads = 0

    @property
    def position(self) -> int:
        return self._source.position

    def next_partition(self) -> list[tuple]:
        self.reads += 1
        return self._source.next_partition()


class UnderReportingSource:
    """A table with one partition more than it admits. An engine using this source removes
    every chunk exactly one iteration before it has seen the whole table."""

    def __init__(self, source: ListPartitionSource) -> None:
        self._source = source
        self.partitions = source.partitions - 1

    @property
    def position(self) -> int:
        return self._source.position

    def next_partition(self) -> list[tuple]:
        return self._source.next_partition()


# ---------------------------------------------------------------------------- Tuning


def test_the_tuned_sources_have_the_partition_counts_of_the_real_tables() -> None:
    engine = build_engine()
    assert engine.customer_partitions == NC
    assert engine.product_partitions == NP
    assert len(EVERY_ENTRY_PHASE) == 72


# ------------------------------------------------- Both passes, from every entry phase


@pytest.mark.parametrize(("customer_phase", "product_phase"), EVERY_ENTRY_PHASE)
def test_stream_is_enriched_by_both_passes_from_every_entry_phase(
    customer_phase: int, product_phase: int
) -> None:
    engine = build_engine(customer_phase=customer_phase, product_phase=product_phase)
    assert engine.customer_position == customer_phase
    assert engine.product_position == product_phase
    stream = stream_covering_all_master_data()

    output = rows_of(run_to_completion(engine, stream))

    assert len(output) == len(stream)
    for transaction in output:
        assert_fully_enriched(transaction)
    assert engine.h_size == 0
    assert engine.queue_len == 0


@pytest.mark.parametrize(("customer_phase", "product_phase"), EVERY_ENTRY_PHASE)
def test_one_chunk_meets_all_of_both_tables_from_every_entry_phase(
    customer_phase: int, product_phase: int
) -> None:
    """A single chunk holds a row for every customer and every product, so it passes only
    if the one chunk really sees every partition of both tables, whatever its entry phase."""
    stream = stream_covering_all_master_data()
    engine = build_engine(
        customer_phase=customer_phase,
        product_phase=product_phase,
        limit=len(stream),
        w=len(stream),
    )

    finished = run_to_completion(engine, stream)

    assert len(finished) == 1
    assert finished[0].customer_visits == NC
    assert finished[0].product_visits == NC
    for transaction in finished[0].transactions:
        assert_fully_enriched(transaction)


def test_a_single_run_passes_through_all_72_phases_and_loses_nothing() -> None:
    """The cursors reach every phase on their own in one run, with chunks entering at each."""
    engine = build_engine()
    stream: list[Transaction] = []
    finished: list[Chunk] = []
    phases: set[tuple[int, int]] = set()
    for sequence in range(72 * 3):
        customer = CUSTOMERS[sequence * 79 % len(CUSTOMERS)][0]
        row = make_transaction(sequence, customer, PRODUCTS[sequence * 83 % len(PRODUCTS)][0])
        stream.append(row)
        engine.stream_buffer.put_online([row])
        finished.extend(engine.run_iteration())
        phases.add((engine.customer_position, engine.product_position))
    while engine.has_work():
        finished.extend(engine.run_iteration())

    assert len(phases) == 72
    assert rows_of(finished) == stream
    for transaction in stream:
        assert_fully_enriched(transaction)


# ------------------------------------------------------------- Removal timing (step 4)


@pytest.mark.parametrize(("customer_phase", "product_phase"), EVERY_ENTRY_PHASE)
def test_the_row_seen_last_is_matched_only_on_the_final_iteration(
    customer_phase: int, product_phase: int
) -> None:
    customer_tuple = tuple_in_partition(CUSTOMERS, partition_seen_last(customer_phase, NC))
    product_tuple = tuple_in_partition(PRODUCTS, partition_seen_last(product_phase, NP))
    row = make_transaction(1, customer_tuple[0], product_tuple[0])
    engine = build_engine(customer_phase=customer_phase, product_phase=product_phase)
    engine.stream_buffer.put_online([row])

    for _ in range(NC - 1):
        assert engine.run_iteration() == []

    # One iteration short of the customer table: removing the chunk now would lose this.
    assert row.customer is None
    assert row.product is product_tuple
    assert engine.queue_len == 1

    finished = engine.run_iteration()

    assert rows_of(finished) == [row]
    assert row.customer is customer_tuple
    assert engine.queue_len == 0


def test_removing_a_chunk_one_iteration_early_loses_a_customer_match() -> None:
    """Shows the loss itself: the same row, with removal one iteration sooner."""
    customer_id = tuple_in_partition(CUSTOMERS, NC)[0]
    product_id = PRODUCTS[0][0]

    correct = build_engine()
    kept_row = make_transaction(1, customer_id, product_id)
    correct.stream_buffer.put_online([kept_row])
    correct_output = [correct.run_iteration() for _ in range(NC)]

    early = MeshJoinEngine(
        StreamBuffer(),
        UnderReportingSource(ListPartitionSource(CUSTOMERS, SIZE)),
        ListPartitionSource(PRODUCTS, SIZE),
    )
    lost_row = make_transaction(1, customer_id, product_id)
    early.stream_buffer.put_online([lost_row])
    early_output = [early.run_iteration() for _ in range(NC - 1)]

    assert rows_of(correct_output[-1]) == [kept_row]
    assert kept_row.is_enriched
    assert rows_of(early_output[-1]) == [lost_row]
    assert lost_row.customer is None
    assert not lost_row.is_enriched


def test_removing_a_chunk_one_iteration_early_loses_a_product_match() -> None:
    """The same proof for the product pass, with product as the longer table."""
    customers = customer_rows(4000)
    products = product_rows(4338)
    product_id = tuple_in_partition(products, 9)[0]
    customer_id = customers[0][0]

    correct = build_engine(customers, products)
    kept_row = make_transaction(1, customer_id, product_id)
    correct.stream_buffer.put_online([kept_row])
    correct_output = [correct.run_iteration() for _ in range(9)]

    early = MeshJoinEngine(
        StreamBuffer(),
        ListPartitionSource(customers, SIZE),
        UnderReportingSource(ListPartitionSource(products, SIZE)),
    )
    lost_row = make_transaction(1, customer_id, product_id)
    early.stream_buffer.put_online([lost_row])
    early_output = [early.run_iteration() for _ in range(8)]

    assert rows_of(correct_output[-1]) == [kept_row]
    assert kept_row.is_enriched
    assert rows_of(early_output[-1]) == [lost_row]
    assert lost_row.product is None


@pytest.mark.parametrize(
    ("customer_count", "product_count", "iterations"),
    [
        (4338, 3665, 9),  # the real tables: customers are the longer scan
        (3665, 4338, 9),  # products are the longer scan
        (1000, 3500, 7),
        (3500, 1000, 7),
    ],
)
def test_a_chunk_stays_until_it_has_seen_both_whole_tables(
    customer_count: int, product_count: int, iterations: int
) -> None:
    customers = customer_rows(customer_count)
    products = product_rows(product_count)
    engine = build_engine(customers, products)
    last_customer = customers[-1]
    last_product = products[-1]
    row = make_transaction(1, last_customer[0], last_product[0])
    engine.stream_buffer.put_online([row])

    for _ in range(iterations - 1):
        assert engine.run_iteration() == []
    finished = engine.run_iteration()

    assert rows_of(finished) == [row]
    assert row.customer is last_customer
    assert row.product is last_product
    assert finished[0].customer_visits == iterations
    assert finished[0].product_visits == iterations


def test_nc_smaller_than_np_reads_customer_partitions_again_harmlessly() -> None:
    customers = customer_rows(1000)
    products = product_rows(4000)
    engine = build_engine(customers, products)
    assert (engine.customer_partitions, engine.product_partitions) == (2, 8)
    stream = [
        make_transaction(i, customers[i % 1000][0], products[i % 4000][0]) for i in range(4000)
    ]

    output = rows_of(run_to_completion(engine, stream))

    assert output == stream
    for transaction in output:
        assert_fully_enriched(transaction)


def test_a_repeat_visit_leaves_the_enrichment_unchanged() -> None:
    """Product partition 1 comes round twice in a chunk's nine iterations."""
    product_tuple = tuple_in_partition(PRODUCTS, 1)
    row = make_transaction(1, CUSTOMERS[0][0], product_tuple[0])
    engine = build_engine()
    engine.stream_buffer.put_online([row])

    engine.run_iteration()
    assert row.product is product_tuple
    while engine.has_work():
        engine.run_iteration()

    assert row.product is product_tuple
    assert engine.product_position == 1


# ---------------------------------------------------------- Output order and exactness


def test_one_output_per_transaction_including_exact_duplicates() -> None:
    stream = stream_covering_all_master_data()
    twin_rows = [
        make_transaction(i + 10_000, CUSTOMERS[7][0], PRODUCTS[7][0]) for i in range(1200)
    ]
    stream.extend(twin_rows)

    output = rows_of(run_to_completion(build_engine(), stream))

    assert Counter(id(transaction) for transaction in output) == Counter(
        id(transaction) for transaction in stream
    )
    for twin in twin_rows:
        assert twin.customer is CUSTOMERS[7]
        assert twin.product is PRODUCTS[7]


def test_chunks_leave_in_first_in_first_out_order_after_a_full_scan() -> None:
    rng = random.Random(72)
    engine = build_engine()
    entered: dict[int, int] = {}
    left: dict[int, int] = {}
    leaving_order: list[int] = []
    sequence = 0
    for iteration in range(400):
        burst = rng.choice([0, 0, 1, 40, 500, 500])
        for _ in range(burst):
            engine.stream_buffer.put_online(
                [make_transaction(sequence, CUSTOMERS[0][0], PRODUCTS[0][0])], timeout=0
            )
            sequence += 1
        for chunk in engine.run_iteration():
            leaving_order.append(chunk.id)
            left[chunk.id] = iteration
        newest = next(iter(engine.queue), None)
        if newest is not None:
            entered.setdefault(newest.id, iteration)

    assert leaving_order == sorted(leaving_order)
    assert len(leaving_order) > 100
    for chunk_id, left_at in left.items():
        assert left_at - entered[chunk_id] == NC - 1


def test_output_order_is_the_arrival_order_of_the_stream() -> None:
    """The resume offset counts online rows in file order, so the join must not reorder."""
    stream = stream_covering_all_master_data()
    assert rows_of(run_to_completion(build_engine(), stream)) == stream


def test_online_rows_keep_file_order_while_mixed_with_periodic_and_shed() -> None:
    rng = random.Random(23)
    engine = build_engine()
    buffer = engine.stream_buffer
    online = [
        make_transaction(i, CUSTOMERS[i][0], PRODUCTS[i % 3665][0]) for i in range(len(CUSTOMERS))
    ]
    periodic = [
        make_transaction(i, CUSTOMERS[i][0], PRODUCTS[i][0], PROCESSING_PERIODIC)
        for i in range(1500)
    ]
    shed = [
        make_transaction(i, CUSTOMERS[i][0], PRODUCTS[i][0], PROCESSING_SHED) for i in range(800)
    ]
    finished: list[Chunk] = []
    online_at = periodic_at = shed_at = 0
    while (
        engine.has_work()
        or online_at < len(online)
        or periodic_at < len(periodic)
        or shed_at < len(shed)
    ):
        arrivals = online[online_at : online_at + rng.randint(0, 400)]
        online_at += buffer.put_online(arrivals, timeout=0)
        slots = buffer.reserve_slots(rng.randint(0, 200), keep_free=0)
        rows = periodic[periodic_at : periodic_at + slots]
        periodic_at += len(rows)
        buffer.put_reserved(LANE_PERIODIC, rows, slots)
        slots = buffer.reserve_slots(rng.randint(0, 100), keep_free=0)
        rows = shed[shed_at : shed_at + slots]
        shed_at += len(rows)
        buffer.put_reserved(LANE_SHED, rows, slots)
        finished.extend(engine.run_iteration())

    output = rows_of(finished)
    assert [row for row in output if row.processing_mode == PROCESSING_LIVE] == online
    assert [row for row in output if row.processing_mode == PROCESSING_PERIODIC] == periodic
    assert [row for row in output if row.processing_mode == PROCESSING_SHED] == shed
    for chunk in finished:
        modes = [row.processing_mode for row in chunk.transactions]
        rank = {PROCESSING_LIVE: 0, PROCESSING_PERIODIC: 1, PROCESSING_SHED: 2}
        assert modes == sorted(modes, key=rank.__getitem__)


def test_a_chunk_is_built_with_online_rows_first() -> None:
    engine = build_engine()
    buffer = engine.stream_buffer
    periodic = make_transaction(1, CUSTOMERS[0][0], PRODUCTS[0][0], PROCESSING_PERIODIC)
    shed = make_transaction(2, CUSTOMERS[0][0], PRODUCTS[0][0], PROCESSING_SHED)
    online = make_transaction(3, CUSTOMERS[0][0], PRODUCTS[0][0])
    buffer.reserve_slots(2)
    buffer.put_reserved(LANE_SHED, [shed], 1)
    buffer.put_reserved(LANE_PERIODIC, [periodic], 1)
    buffer.put_online([online])

    engine.run_iteration()

    assert engine.queue.oldest().transactions == [online, periodic, shed]


def test_the_join_changes_only_the_two_master_data_fields() -> None:
    row = make_transaction(41, CUSTOMERS[3][0], PRODUCTS[3][0], PROCESSING_PERIODIC)
    row.staging_id = 77
    row.enqueued_at = 123.5
    engine = build_engine()
    engine.stream_buffer.put_online([row])

    while engine.has_work():
        engine.run_iteration()

    assert row.transaction_id == "41"
    assert row.customer_id == CUSTOMERS[3][0]
    assert row.product_id == PRODUCTS[3][0]
    assert row.quantity == 42
    assert row.unit_price == Decimal("2.550")
    assert row.event_time == datetime(2011, 3, 1, 8, 30)
    assert row.sales_mode == SALES_MODE_PERIODIC
    assert row.processing_mode == PROCESSING_PERIODIC
    assert row.staging_id == 77
    assert row.enqueued_at == 123.5


# ---------------------------------------------------------------------- Memory limits


def test_memory_limits_hold_over_a_long_randomized_run() -> None:
    """Measured in the middle of each iteration, right after a chunk enters H, because that
    is when H and the Queue are at their largest. The run must really reach the limits."""
    rng = random.Random(20261113)
    engine = build_engine()
    buffer = engine.stream_buffer
    peak = {"h": 0, "queue": 0}
    enter_queue = engine.queue.append

    def measured_append(chunk: Chunk) -> None:
        enter_queue(chunk)
        peak["h"] = max(peak["h"], engine.h_size)
        peak["queue"] = max(peak["queue"], engine.queue_len)

    engine.queue.append = measured_append  # type: ignore[method-assign]

    stream: list[Transaction] = []
    finished: list[Chunk] = []
    fed = 0
    for _ in range(1500):
        burst = rng.choice([0, 0, 1, 37, 499, 500, 700, 1500])
        for sequence in range(len(stream), len(stream) + burst):
            stream.append(
                make_transaction(
                    sequence,
                    CUSTOMERS[rng.randrange(len(CUSTOMERS))][0],
                    PRODUCTS[rng.randrange(len(PRODUCTS))][0],
                )
            )
        fed += buffer.put_online(stream[fed:], timeout=0)
        finished.extend(engine.run_iteration())

        assert engine.h_size <= 4000
        assert engine.queue_len <= NC - 1
        assert engine.customer_disk_buffer_size <= SIZE
        assert engine.product_disk_buffer_size <= SIZE
        counts = buffer.counts()
        assert counts.total + counts.reserved <= 1000

    while fed < len(stream) or engine.has_work():
        fed += buffer.put_online(stream[fed:], timeout=0)
        finished.extend(engine.run_iteration())

    assert peak == {"h": 4500, "queue": 9}
    output = rows_of(finished)
    assert output == stream
    for transaction in output:
        assert_fully_enriched(transaction)
    assert all(len(chunk) <= 500 for chunk in finished)


def test_h_and_queue_hold_references_to_the_chunk_and_not_copies() -> None:
    row = make_transaction(1, CUSTOMERS[0][0], PRODUCTS[0][0])
    engine = build_engine()
    engine.stream_buffer.put_online([row])

    engine.run_iteration()

    chunk = engine.queue.oldest()
    assert chunk.transactions[0] is row
    assert engine.hash_table.by_customer[row.customer_id][0] is row
    assert engine.hash_table.by_product[row.product_id][0] is row


# ------------------------------------------------------------------- Partition shapes


def test_a_last_partition_smaller_than_500_is_read_and_joined() -> None:
    engine = build_engine()
    customer_sizes = []
    product_sizes = []
    for _ in range(NC * NP):
        engine.run_iteration()
        customer_sizes.append(engine.customer_disk_buffer_size)
        product_sizes.append(engine.product_disk_buffer_size)
    assert customer_sizes[:NC] == [500] * 8 + [338]
    assert product_sizes[:NP] == [500] * 7 + [165]
    assert customer_sizes == (customer_sizes[:NC] * NP)
    assert product_sizes == (product_sizes[:NP] * NC)

    row = make_transaction(1, CUSTOMERS[-1][0], PRODUCTS[-1][0])
    engine.stream_buffer.put_online([row])
    while engine.has_work():
        engine.run_iteration()
    assert row.customer is CUSTOMERS[-1]
    assert row.product is PRODUCTS[-1]


def test_a_table_exactly_divisible_by_500_has_no_empty_extra_partition() -> None:
    customers = customer_rows(1000)
    products = product_rows(1500)
    engine = build_engine(customers, products)
    assert (engine.customer_partitions, engine.product_partitions) == (2, 3)
    stream = [
        make_transaction(i, customers[i % 1000][0], products[i % 1500][0]) for i in range(3000)
    ]
    positions = []

    finished: list[Chunk] = []
    position = 0
    while position < len(stream) or engine.has_work():
        position += engine.stream_buffer.put_online(stream[position:], timeout=0)
        finished.extend(engine.run_iteration())
        positions.append((engine.customer_position, engine.product_position))
        assert engine.customer_disk_buffer_size == 500
        assert engine.product_disk_buffer_size == 500

    assert positions[:7] == [(1, 1), (2, 2), (1, 3), (2, 1), (1, 2), (2, 3), (1, 1)]
    for transaction in rows_of(finished):
        assert_fully_enriched(transaction)
    assert finished[0].customer_visits == 3
    assert finished[0].product_visits == 3


@pytest.mark.parametrize(
    ("customer_count", "product_count"), [(1, 1), (500, 500), (1, 500), (500, 1), (17, 3)]
)
def test_single_partition_tables_finish_a_chunk_in_one_iteration(
    customer_count: int, product_count: int
) -> None:
    customers = customer_rows(customer_count)
    products = product_rows(product_count)
    engine = build_engine(customers, products)
    assert (engine.customer_partitions, engine.product_partitions) == (1, 1)
    rows = [
        make_transaction(i, customers[i % customer_count][0], products[i % product_count][0])
        for i in range(500)
    ]
    engine.stream_buffer.put_online(rows)

    finished = engine.run_iteration()

    assert rows_of(finished) == rows
    for transaction in rows:
        assert_fully_enriched(transaction)
    assert engine.h_size == 0
    assert engine.queue_len == 0
    assert engine.run_iteration() == []
    assert engine.customer_position == 1
    assert engine.product_position == 1


def test_a_single_partition_table_joined_with_a_large_one() -> None:
    customers = customer_rows(4338)
    products = product_rows(3)
    engine = build_engine(customers, products)
    assert (engine.customer_partitions, engine.product_partitions) == (9, 1)
    row = make_transaction(1, customers[-1][0], products[2][0])
    engine.stream_buffer.put_online([row])

    for _ in range(8):
        assert engine.run_iteration() == []
    assert rows_of(engine.run_iteration()) == [row]
    assert_fully_enriched(row)


def test_a_partition_larger_than_the_disk_buffer_is_refused() -> None:
    engine = MeshJoinEngine(
        StreamBuffer(), ListPartitionSource(CUSTOMERS, 501), ListPartitionSource(PRODUCTS, SIZE)
    )
    with pytest.raises(ValueError, match="Disk buffer"):
        engine.run_iteration()


# --------------------------------------------------------- Unmatched rows, slow streams


def test_unmatched_rows_surface_as_not_enriched() -> None:
    no_customer = make_transaction(1, "NO-SUCH-CUSTOMER", PRODUCTS[0][0])
    no_product = make_transaction(2, CUSTOMERS[0][0], "NO-SUCH-PRODUCT")
    neither = make_transaction(3, "NO-SUCH-CUSTOMER", "NO-SUCH-PRODUCT")
    matched = make_transaction(4, CUSTOMERS[0][0], PRODUCTS[0][0])
    stream = [no_customer, no_product, neither, matched]

    output = rows_of(run_to_completion(build_engine(), stream))

    assert output == stream
    assert no_customer.customer is None
    assert no_customer.product is PRODUCTS[0]
    assert not no_customer.is_enriched
    assert no_product.customer is CUSTOMERS[0]
    assert no_product.product is None
    assert not no_product.is_enriched
    assert not neither.is_enriched
    assert matched.is_enriched


def test_ids_differing_only_in_case_get_their_own_tuples() -> None:
    upper = ("15056BL", "Upper", "Toys", "SUP01", "Northwind Supplies")
    lower = ("15056bl", "Lower", "Toys", "SUP01", "Northwind Supplies")
    only_upper = ("85123A", "White Heart", "Toys", "SUP01", "Northwind Supplies")
    engine = build_engine(CUSTOMERS[:10], [upper, lower, only_upper])
    upper_row = make_transaction(1, CUSTOMERS[0][0], "15056BL")
    lower_row = make_transaction(2, CUSTOMERS[0][0], "15056bl")
    wrong_case = make_transaction(3, CUSTOMERS[0][0], "85123a")

    run_to_completion(engine, [upper_row, lower_row, wrong_case])

    assert upper_row.product is upper
    assert lower_row.product is lower
    assert wrong_case.product is None


def test_empty_stream_iterations_only_move_the_cursors() -> None:
    engine = build_engine()

    expected_positions = []
    actual_positions = []
    for iteration in range(100):
        assert engine.run_iteration() == []
        expected_positions.append((iteration % NC + 1, iteration % NP + 1))
        actual_positions.append((engine.customer_position, engine.product_position))

    assert actual_positions == expected_positions
    assert engine.h_size == 0
    assert engine.queue_len == 0
    assert not engine.has_work()


def test_a_chunk_entering_after_an_idle_spell_is_still_fully_joined() -> None:
    engine = build_engine()
    for _ in range(13):
        engine.run_iteration()
    stream = stream_covering_all_master_data()

    for transaction in rows_of(run_to_completion(engine, stream)):
        assert_fully_enriched(transaction)


def test_chunks_in_memory_keep_scanning_while_the_stream_is_silent() -> None:
    row = make_transaction(1, CUSTOMERS[-1][0], PRODUCTS[-1][0])
    engine = build_engine()
    engine.stream_buffer.put_online([row])

    results = [engine.run_iteration() for _ in range(NC)]

    assert [rows_of(result) for result in results] == [[]] * (NC - 1) + [[row]]
    assert_fully_enriched(row)


@pytest.mark.parametrize("size", [1, 2, 37, 499, 500])
def test_partial_chunks_are_joined_like_full_ones(size: int) -> None:
    engine = build_engine()
    rows = [
        make_transaction(i, CUSTOMERS[i * 8 + 3][0], PRODUCTS[i * 7 + 1][0]) for i in range(size)
    ]
    engine.stream_buffer.put_online(rows)

    finished = run_to_completion(engine, [])

    assert [len(chunk) for chunk in finished] == [size]
    for transaction in rows:
        assert_fully_enriched(transaction)


def test_chunks_of_different_sizes_with_gaps_between_them() -> None:
    engine = build_engine()
    finished: list[Chunk] = []
    expected: list[Transaction] = []
    sequence = 0
    for size in (500, 0, 1, 0, 0, 133, 500, 0, 7, 250):
        rows = [
            make_transaction(i, CUSTOMERS[i * 13 % 4338][0], PRODUCTS[i * 17 % 3665][0])
            for i in range(sequence, sequence + size)
        ]
        sequence += size
        expected.extend(rows)
        engine.stream_buffer.put_online(rows)
        finished.extend(engine.run_iteration())
    while engine.has_work():
        finished.extend(engine.run_iteration())

    assert [len(chunk) for chunk in finished] == [500, 1, 133, 500, 7, 250]
    assert rows_of(finished) == expected
    for transaction in expected:
        assert_fully_enriched(transaction)


def test_a_full_stream_buffer_gives_chunks_of_exactly_w() -> None:
    engine = build_engine()
    rows = [make_transaction(i, CUSTOMERS[0][0], PRODUCTS[0][0]) for i in range(1000)]
    engine.stream_buffer.put_online(rows)

    engine.run_iteration()
    assert engine.h_size == 500
    assert engine.stream_buffer.counts().total == 500

    engine.run_iteration()
    assert engine.h_size == 1000
    assert engine.queue_len == 2
    assert engine.stream_buffer.counts().total == 0


# ---------------------------------------------------- The rules on how R may be touched


def test_each_iteration_reads_one_partition_of_each_table_however_much_is_in_h() -> None:
    customer_source = ReadCountingSource(ListPartitionSource(CUSTOMERS, SIZE))
    product_source = ReadCountingSource(ListPartitionSource(PRODUCTS, SIZE))
    engine = MeshJoinEngine(StreamBuffer(), customer_source, product_source)
    engine.stream_buffer.put_online(stream_covering_all_master_data()[:1000])

    for iteration in range(1, 21):
        engine.run_iteration()
        assert customer_source.reads == iteration
        assert product_source.reads == iteration


def test_h_is_keyed_by_transactions_in_memory_and_holds_no_master_data() -> None:
    row = make_transaction(1, CUSTOMERS[0][0], PRODUCTS[0][0])
    engine = build_engine()
    engine.stream_buffer.put_online([row])

    for _ in range(NC - 1):
        engine.run_iteration()
        assert list(engine.hash_table.by_customer) == [row.customer_id]
        assert list(engine.hash_table.by_product) == [row.product_id]


def test_the_disk_buffer_is_replaced_by_each_partition() -> None:
    engine = build_engine()

    engine.run_iteration()
    first = list(engine.customer_disk_buffer)
    engine.run_iteration()
    second = list(engine.customer_disk_buffer)

    assert first == CUSTOMERS[:500]
    assert second == CUSTOMERS[500:1000]


def test_join_attribute_position_comes_from_the_key_index() -> None:
    customers = [("Female", "C1"), ("Male", "C2")]
    products = [("Teapot", "Toys", "P1")]
    engine = MeshJoinEngine(
        StreamBuffer(),
        ListPartitionSource(customers, SIZE),
        ListPartitionSource(products, SIZE),
        customer_key_index=1,
        product_key_index=2,
    )
    row = make_transaction(1, "C2", "P1")
    engine.stream_buffer.put_online([row])

    engine.run_iteration()

    assert row.customer is customers[1]
    assert row.product is products[0]


def test_importing_the_engine_needs_no_database_driver() -> None:
    """The join itself must stay DB-free, so it is checked in a fresh interpreter."""
    project = Path(__file__).resolve().parents[2]
    check = (
        "import sys; import retail_dw.meshjoin.engine; "
        "sys.exit(any(name.split('.')[0] == 'mysql' for name in sys.modules))"
    )
    result = subprocess.run([sys.executable, "-c", check], cwd=project, timeout=60)
    assert result.returncode == 0


def test_a_list_source_satisfies_the_partition_source_shape() -> None:
    source: PartitionSource = ListPartitionSource(customer_rows(3), SIZE)
    assert source.partitions == 1
    assert source.position == 0
    assert source.next_partition() == customer_rows(3)
    assert source.position == 1


# ---------------------------------------------------------------- Engine bookkeeping


def test_has_work_follows_the_stream_buffer_and_h() -> None:
    row = make_transaction(1, CUSTOMERS[0][0], PRODUCTS[0][0])
    engine = build_engine()
    assert not engine.has_work()

    engine.stream_buffer.put_online([row])
    assert engine.has_work()

    engine.run_iteration()
    assert engine.stream_buffer.counts().total == 0
    assert engine.has_work()

    for _ in range(NC - 2):
        engine.run_iteration()
    assert engine.has_work()

    assert rows_of(engine.run_iteration()) == [row]
    assert not engine.has_work()


def test_a_reservation_alone_is_not_work() -> None:
    engine = build_engine()
    engine.stream_buffer.reserve_slots(5)
    assert not engine.has_work()


def test_status_values_before_and_during_a_run() -> None:
    engine = build_engine()
    assert engine.h_size == 0
    assert engine.queue_len == 0
    assert engine.customer_position == 0
    assert engine.product_position == 0
    assert engine.customer_disk_buffer_size == 0
    assert engine.product_disk_buffer_size == 0

    engine.stream_buffer.put_online([make_transaction(1, CUSTOMERS[0][0], PRODUCTS[0][0])])
    engine.run_iteration()

    assert engine.h_size == 1
    assert engine.queue_len == 1
    assert engine.customer_position == 1
    assert engine.product_position == 1
    assert engine.customer_disk_buffer_size == 500
    assert engine.product_disk_buffer_size == 500


@pytest.mark.parametrize("w", [0, -1])
def test_invalid_chunk_size_is_refused(w: int) -> None:
    with pytest.raises(ValueError, match="chunk size"):
        build_engine(w=w)


# -------------------------------------------------------------- Queue and Disk buffer


def test_queue_gives_the_oldest_chunk_first_and_keeps_it_at_the_end() -> None:
    queue = ChunkQueue()
    chunks = [Chunk(i, []) for i in range(3)]
    for chunk in chunks:
        queue.append(chunk)

    assert len(queue) == 3
    assert list(queue) == [chunks[2], chunks[1], chunks[0]]
    assert queue.oldest() is chunks[0]
    assert queue.pop_oldest() is chunks[0]
    assert queue.oldest() is chunks[1]
    assert len(queue) == 2


def test_empty_queue_has_no_oldest_chunk() -> None:
    queue = ChunkQueue()
    assert queue.oldest() is None
    assert len(queue) == 0
    with pytest.raises(IndexError):
        queue.pop_oldest()


def test_disk_buffer_holds_one_partition_up_to_its_capacity() -> None:
    disk_buffer = DiskBuffer()
    assert len(disk_buffer) == 0

    disk_buffer.load(CUSTOMERS[:500])
    assert len(disk_buffer) == 500

    disk_buffer.load(CUSTOMERS[:3])
    assert list(disk_buffer) == CUSTOMERS[:3]


def test_disk_buffer_refuses_501_tuples_and_keeps_what_it_held() -> None:
    disk_buffer = DiskBuffer()
    disk_buffer.load(CUSTOMERS[:2])

    with pytest.raises(ValueError, match="500"):
        disk_buffer.load(CUSTOMERS[:501])

    assert list(disk_buffer) == CUSTOMERS[:2]


def test_list_source_wraps_after_the_last_partition() -> None:
    source = ListPartitionSource(customer_rows(5), 2)
    assert source.partitions == 3

    pages = [source.next_partition() for _ in range(4)]

    rows = customer_rows(5)
    assert pages == [rows[:2], rows[2:4], rows[4:], rows[:2]]
    assert source.position == 1


def test_list_source_refuses_an_empty_table_and_a_bad_size() -> None:
    with pytest.raises(EmptyMasterDataError):
        ListPartitionSource([], SIZE)
    with pytest.raises(ValueError, match="size"):
        ListPartitionSource(customer_rows(1), 0)
