"""Stream buffer: priority, the capacity invariant, blocking and reservations.

The blocking tests use real threads, because a blocked producer is the behaviour under
test. The buffer never looks inside an item, so plain values stand in for transactions.
"""

from __future__ import annotations

import random
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from retail_dw.meshjoin.stream_buffer import (
    LANE_PERIODIC,
    LANE_SHED,
    BufferCounts,
    StreamBuffer,
)

# An upper bound on how long a healthy thread takes to respond. It is only ever reached
# when a test is about to fail, so it can be generous without slowing a passing run.
RESPONSE_SECONDS = 30.0
# How long a call must stay blocked to count as blocked.
BLOCKED_SECONDS = 0.2


class Background:
    """A call running on a real thread, so that a test can watch it block and resume."""

    def __init__(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        self.result: Any = None
        self._error: BaseException | None = None
        self._done = threading.Event()

        def run() -> None:
            try:
                self.result = fn(*args, **kwargs)
            except BaseException as exc:
                self._error = exc
            finally:
                self._done.set()

        # Daemon, so that a test failing while a call is blocked cannot hang the run.
        threading.Thread(target=run, daemon=True).start()

    def assert_blocked(self) -> None:
        assert not self._done.wait(BLOCKED_SECONDS), "call returned but should have blocked"

    def finish(self) -> Any:
        assert self._done.wait(RESPONSE_SECONDS), "call is still blocked"
        if self._error is not None:
            raise self._error
        return self.result


@pytest.fixture
def frequent_thread_switches() -> Iterator[None]:
    """Make the interpreter switch threads about 500 times more often than it normally
    does, so that thousands of operations really do interleave instead of each thread
    finishing its work inside a few long time slices."""
    previous = sys.getswitchinterval()
    sys.setswitchinterval(1e-5)
    yield
    sys.setswitchinterval(previous)


def full_buffer(limit: int = 1000) -> StreamBuffer:
    buffer = StreamBuffer(limit=limit, online_reserve=0)
    assert buffer.put_online(list(range(limit))) == limit
    return buffer


def test_take_serves_online_then_periodic_then_shed() -> None:
    buffer = StreamBuffer()
    slots = buffer.reserve_slots(4)
    buffer.put_reserved(LANE_SHED, ["s1", "s2"], 2)
    buffer.put_reserved(LANE_PERIODIC, ["p1", "p2"], 2)
    assert slots == 4
    buffer.put_online(["o1", "o2"])

    assert buffer.take(10) == ["o1", "o2", "p1", "p2", "s1", "s2"]


def test_each_lane_is_first_in_first_out() -> None:
    buffer = StreamBuffer()
    buffer.reserve_slots(4)
    buffer.put_reserved(LANE_PERIODIC, [1, 2], 2)
    buffer.put_reserved(LANE_PERIODIC, [3, 4], 2)
    buffer.put_online(["a", "b"])
    buffer.put_online(["c"])

    assert buffer.take(7) == ["a", "b", "c", 1, 2, 3, 4]


def test_new_online_rows_overtake_periodic_rows_already_waiting() -> None:
    buffer = StreamBuffer()
    buffer.reserve_slots(3)
    buffer.put_reserved(LANE_PERIODIC, ["p1", "p2", "p3"], 3)
    assert buffer.take(1) == ["p1"]

    buffer.put_online(["o1"])

    assert buffer.take(2) == ["o1", "p2"]


def test_take_fills_a_chunk_across_lanes_up_to_max_n() -> None:
    buffer = StreamBuffer()
    buffer.put_online([f"o{i}" for i in range(400)])
    granted = buffer.reserve_slots(200)
    buffer.put_reserved(LANE_PERIODIC, [f"p{i}" for i in range(200)], granted)

    chunk = buffer.take(500)

    assert len(chunk) == 500
    assert chunk[399] == "o399"
    assert chunk[400] == "p0"
    assert buffer.counts() == BufferCounts(online=0, periodic=100, shed=0, reserved=0, total=100)


def test_take_defaults_to_chunk_size_w() -> None:
    buffer = full_buffer()
    assert len(buffer.take()) == 500


def test_counts_reports_each_lane_and_reservations() -> None:
    buffer = StreamBuffer()
    buffer.put_online([1, 2, 3])
    buffer.reserve_slots(5)
    buffer.put_reserved(LANE_PERIODIC, ["p"], 1)
    buffer.put_reserved(LANE_SHED, ["s", "t"], 2)

    assert buffer.counts() == BufferCounts(online=3, periodic=1, shed=2, reserved=2, total=6)


@pytest.mark.usefixtures("frequent_thread_switches")
def test_capacity_invariant_under_heavy_concurrent_use() -> None:
    """Many producers and consumers at once: the limit holds at every reading, and every
    item comes out exactly once and in order for its producer."""
    limit = 1000
    online_items = 8000
    reserved_items = 6000
    buffer = StreamBuffer(limit=limit, online_reserve=100)
    violations: list[BufferCounts] = []
    consuming = threading.Event()
    producers_done = threading.Event()

    def check() -> None:
        counts = buffer.counts()
        consistent = counts.total == counts.online + counts.periodic + counts.shed
        if not consistent or counts.total + counts.reserved > limit or counts.reserved < 0:
            violations.append(counts)

    def online_producer(name: str, seed: int) -> None:
        rng = random.Random(seed)
        items = [(name, i) for i in range(online_items)]
        position = 0
        while position < online_items:
            batch = items[position : position + rng.randint(1, 60)]
            position += buffer.put_online(batch, timeout=RESPONSE_SECONDS)
            check()

    def reserving_producer(lane: str, name: str, seed: int) -> None:
        rng = random.Random(seed)
        position = 0
        while position < reserved_items:
            granted = buffer.reserve_slots(rng.randint(1, 80))
            check()
            if granted == 0:
                time.sleep(0.0005)
                continue
            used = min(rng.randint(0, granted), reserved_items - position)
            batch = [(name, i) for i in range(position, position + used)]
            assert buffer.put_reserved(lane, batch, granted) == granted - used
            position += used
            check()

    def consumer(received: list[tuple[str, int]], seed: int) -> None:
        rng = random.Random(seed)
        assert consuming.wait(RESPONSE_SECONDS)
        while True:
            taken = buffer.take(rng.randint(1, 120))
            check()
            if taken:
                received.extend(taken)
            elif producers_done.is_set() and buffer.counts().total == 0:
                return
            else:
                time.sleep(0.0005)

    received_by_consumer: list[list[tuple[str, int]]] = [[], [], []]
    producers = [
        Background(online_producer, "online-a", 1),
        Background(online_producer, "online-b", 2),
        Background(online_producer, "online-c", 3),
        Background(reserving_producer, LANE_PERIODIC, "periodic", 4),
        Background(reserving_producer, LANE_SHED, "shed", 5),
    ]
    consumers = [
        Background(consumer, received, seed)
        for seed, received in enumerate(received_by_consumer, start=6)
    ]

    # Consumers start only once the producers have pressed the buffer to its limit, so
    # the test always exercises a truly full buffer and blocked producers.
    deadline = time.monotonic() + RESPONSE_SECONDS
    while True:
        counts = buffer.counts()
        if counts.total + counts.reserved == limit:
            break
        assert time.monotonic() < deadline, "producers never filled the buffer"
        time.sleep(0.001)
    consuming.set()

    for producer in producers:
        producer.finish()
    producers_done.set()
    for worker in consumers:
        worker.finish()

    assert violations == []
    assert buffer.counts() == BufferCounts(online=0, periodic=0, shed=0, reserved=0, total=0)
    received_all = [item for received in received_by_consumer for item in received]
    expected = [
        (name, i)
        for name, count in (
            ("online-a", online_items),
            ("online-b", online_items),
            ("online-c", online_items),
            ("periodic", reserved_items),
            ("shed", reserved_items),
        )
        for i in range(count)
    ]
    assert Counter(received_all) == Counter(expected)
    # A consumer sees a part of each lane in FIFO order, so sequence numbers only grow.
    for received in received_by_consumer:
        last_seen: dict[str, int] = {}
        for name, sequence in received:
            assert sequence > last_seen.get(name, -1)
            last_seen[name] = sequence


def test_online_blocks_when_full_then_resumes_after_take() -> None:
    buffer = full_buffer()
    producer = Background(buffer.put_online, ["late"])

    producer.assert_blocked()
    assert buffer.counts().online == 1000

    assert len(buffer.take(500)) == 500
    assert producer.finish() == 1
    assert buffer.counts().online == 501


def test_online_blocks_on_reserved_slots_then_resumes_after_release() -> None:
    buffer = StreamBuffer(limit=1000, online_reserve=0)
    assert buffer.reserve_slots(1000) == 1000
    producer = Background(buffer.put_online, ["late"])

    producer.assert_blocked()

    buffer.release_slots(1)
    assert producer.finish() == 1


def test_online_resumes_when_put_reserved_returns_unused_slots() -> None:
    buffer = StreamBuffer(limit=1000, online_reserve=0)
    granted = buffer.reserve_slots(1000)
    producer = Background(buffer.put_online, ["late"])
    producer.assert_blocked()

    assert buffer.put_reserved(LANE_PERIODIC, ["p"] * 400, granted) == 600

    assert producer.finish() == 1


def test_online_batch_bigger_than_the_buffer_finishes_while_a_consumer_drains() -> None:
    buffer = StreamBuffer(limit=1000)
    items = list(range(3500))
    producer = Background(buffer.put_online, items)
    received: list[int] = []
    deadline = time.monotonic() + RESPONSE_SECONDS

    while len(received) < len(items):
        assert time.monotonic() < deadline, "producer stalled"
        received.extend(buffer.take(500))

    assert producer.finish() == 3500
    assert received == items


def test_stop_event_unblocks_a_waiting_producer() -> None:
    buffer = full_buffer()
    stop = threading.Event()
    producer = Background(buffer.put_online, ["a", "b"], stop)
    producer.assert_blocked()

    stop.set()

    assert producer.finish() == 0
    assert buffer.counts().total == 1000


def test_stop_event_reports_how_many_were_accepted_before_stopping() -> None:
    buffer = StreamBuffer(limit=10, online_reserve=0)
    stop = threading.Event()
    producer = Background(buffer.put_online, list(range(15)), stop)
    producer.assert_blocked()

    stop.set()

    assert producer.finish() == 10
    assert buffer.take(10) == list(range(10))


def test_stop_event_does_not_refuse_items_that_fit() -> None:
    buffer = StreamBuffer()
    stop = threading.Event()
    stop.set()

    assert buffer.put_online(["a", "b"], stop) == 2


def test_timeout_returns_the_accepted_count_and_keeps_nothing_else() -> None:
    buffer = StreamBuffer(limit=10, online_reserve=0)

    assert buffer.put_online(list(range(15)), timeout=0.01) == 10
    assert buffer.counts().total == 10


def test_zero_timeout_does_not_wait() -> None:
    buffer = full_buffer(limit=10)

    assert Background(buffer.put_online, ["late"], timeout=0).finish() == 0


def test_reservation_leaves_the_online_reserve_free() -> None:
    buffer = StreamBuffer(limit=1000, online_reserve=100)

    assert buffer.reserve_slots(5000) == 900
    assert buffer.reserve_slots(1) == 0
    assert buffer.counts().reserved == 900

    assert buffer.put_online(list(range(100)), timeout=0) == 100
    assert buffer.put_online(["full now"], timeout=0) == 0


def test_reservation_counts_occupancy_and_earlier_reservations() -> None:
    buffer = StreamBuffer(limit=1000, online_reserve=100)
    buffer.put_online(list(range(300)))

    assert buffer.reserve_slots(250) == 250
    assert buffer.reserve_slots(5000) == 350
    assert buffer.counts() == BufferCounts(online=300, periodic=0, shed=0, reserved=600, total=300)


def test_reservation_is_floored_at_zero_when_online_is_using_the_reserve() -> None:
    buffer = StreamBuffer(limit=1000, online_reserve=100)
    buffer.put_online(list(range(950)))

    assert buffer.reserve_slots(10) == 0
    assert buffer.counts().reserved == 0


def test_keep_free_overrides_the_default_reserve() -> None:
    buffer = StreamBuffer(limit=1000, online_reserve=100)

    assert buffer.reserve_slots(5000, keep_free=250) == 750
    assert buffer.reserve_slots(5000, keep_free=0) == 250


def test_reservation_can_always_be_followed_by_its_rows() -> None:
    """The reason reservations exist: rows marked released on disk must have a slot. Even
    with online pressing on the buffer, a reservation stays usable."""
    buffer = StreamBuffer(limit=1000, online_reserve=100)
    granted = buffer.reserve_slots(900)

    assert buffer.put_online(list(range(100)), timeout=0) == 100
    assert buffer.put_online(["overflow"], timeout=0) == 0

    assert buffer.put_reserved(LANE_PERIODIC, list(range(granted)), granted) == 0
    assert buffer.counts() == BufferCounts(online=100, periodic=900, shed=0, reserved=0, total=1000)


@pytest.mark.usefixtures("frequent_thread_switches")
def test_hammering_reservations_cannot_starve_the_stream() -> None:
    """Reservers ask for every slot all the time while one consumer drains, the way
    MESHJOIN does. Online rows must still get through and come first."""
    buffer = StreamBuffer(limit=1000, online_reserve=100)
    online_rows = [("online", i) for i in range(5000)]
    finished = threading.Event()
    received: list[tuple[str, int]] = []

    def reserver() -> None:
        while not finished.is_set():
            granted = buffer.reserve_slots(10**6)
            if granted:
                buffer.put_reserved(LANE_SHED, [("shed", 0)] * granted, granted)
            else:
                time.sleep(0.0005)

    def consumer() -> None:
        while not finished.is_set():
            received.extend(buffer.take(500))
            time.sleep(0.0005)

    reservers = [Background(reserver), Background(reserver)]
    draining = Background(consumer)
    producer = Background(buffer.put_online, online_rows)

    assert producer.finish() == 5000
    finished.set()
    for worker in (*reservers, draining):
        worker.finish()
    received.extend(buffer.take(1000))

    assert [row for row in received if row[0] == "online"] == online_rows


def test_put_reserved_returns_the_unused_slots_to_the_free_pool() -> None:
    buffer = StreamBuffer(limit=1000, online_reserve=100)
    granted = buffer.reserve_slots(10)

    assert buffer.put_reserved(LANE_SHED, ["a", "b", "c"], granted) == 7
    assert buffer.counts() == BufferCounts(online=0, periodic=0, shed=3, reserved=0, total=3)
    assert buffer.reserve_slots(5000) == 897


def test_release_slots_makes_them_available_again() -> None:
    buffer = StreamBuffer(limit=1000, online_reserve=100)
    buffer.reserve_slots(900)

    buffer.release_slots(400)

    assert buffer.counts().reserved == 500
    assert buffer.reserve_slots(5000) == 400


def test_online_rows_cannot_use_a_reservation() -> None:
    buffer = StreamBuffer()
    buffer.reserve_slots(2)

    with pytest.raises(ValueError, match="lane"):
        buffer.put_reserved("online", ["a"], 2)

    assert buffer.counts() == BufferCounts(online=0, periodic=0, shed=0, reserved=2, total=0)


def test_put_reserved_refuses_more_items_than_slots() -> None:
    buffer = StreamBuffer()
    buffer.reserve_slots(2)

    with pytest.raises(ValueError, match="reserved"):
        buffer.put_reserved(LANE_PERIODIC, ["a", "b", "c"], 2)

    assert buffer.counts() == BufferCounts(online=0, periodic=0, shed=0, reserved=2, total=0)


def test_put_reserved_refuses_slots_that_were_never_reserved() -> None:
    buffer = StreamBuffer()
    buffer.reserve_slots(1)

    with pytest.raises(ValueError, match="reserved"):
        buffer.put_reserved(LANE_PERIODIC, ["a", "b"], 2)

    assert buffer.counts() == BufferCounts(online=0, periodic=0, shed=0, reserved=1, total=0)


def test_release_refuses_more_than_is_reserved() -> None:
    buffer = StreamBuffer()
    buffer.reserve_slots(1)

    with pytest.raises(ValueError, match="reserved"):
        buffer.release_slots(2)
    with pytest.raises(ValueError, match="reserved"):
        buffer.release_slots(-1)

    assert buffer.counts().reserved == 1


def test_zero_sized_requests_change_nothing() -> None:
    buffer = StreamBuffer()
    buffer.put_online(["a"])

    assert buffer.take(0) == []
    assert buffer.reserve_slots(0) == 0
    assert buffer.put_online([]) == 0
    assert buffer.put_reserved(LANE_PERIODIC, [], 0) == 0
    buffer.release_slots(0)

    assert buffer.counts() == BufferCounts(online=1, periodic=0, shed=0, reserved=0, total=1)


def test_over_limit_requests_are_clamped_to_what_exists() -> None:
    buffer = StreamBuffer(limit=1000)

    assert buffer.put_online(list(range(1500)), timeout=0) == 1000
    assert buffer.reserve_slots(10**9) == 0
    assert len(buffer.take(10**9)) == 1000
    assert buffer.take(10**9) == []


def test_take_on_an_empty_buffer_returns_at_once() -> None:
    assert StreamBuffer().take() == []


@pytest.mark.parametrize(
    "call",
    [
        lambda buffer: buffer.take(-1),
        lambda buffer: buffer.reserve_slots(-1),
        lambda buffer: buffer.reserve_slots(1, keep_free=-1),
    ],
)
def test_negative_requests_are_errors(call: Callable[[StreamBuffer], Any]) -> None:
    with pytest.raises(ValueError):
        call(StreamBuffer())


@pytest.mark.parametrize(
    ("limit", "online_reserve"), [(0, 0), (-1, 0), (10, -1), (10, 11)]
)
def test_invalid_sizes_are_refused(limit: int, online_reserve: int) -> None:
    with pytest.raises(ValueError):
        StreamBuffer(limit=limit, online_reserve=online_reserve)


def test_defaults_are_the_project_settings() -> None:
    buffer = StreamBuffer()
    assert buffer.limit == 1000
    assert buffer.online_reserve == 100
