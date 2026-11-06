"""Stream buffer: the waiting area in front of MESHJOIN (PDF 4.1, 4.3 and section 12).

The stream can arrive faster than MESHJOIN completes one iteration, so arriving
transactions wait here. Three threads use the buffer at the same time and none of them
waits for another's turn. Each call holds the one lock only long enough to move items.

The capacity of 1,000 transactions is shared by three FIFO lanes, and they are read in
the priority order the PDF fixes: online first, then periodic, and lastly shed.

Periodic and shed rows need a reservation first, because they leave the disk before
they arrive here. A row that is already marked released has nowhere else to go, so its
slot must exist before that database commit happens. A reservation also keeps the online
reserve free, which is how periodic work is stopped from locking the stream out of the
buffer. Online rows need no reservation: they are the top priority, they come from a file
and not from a table, and when the buffer is full their thread simply waits.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Sequence
from typing import NamedTuple

from retail_dw.config import CHUNK_SIZE_W, ONLINE_RESERVE_SLOTS, STREAM_BUFFER_LIMIT
from retail_dw.meshjoin.model import Transaction

LANE_PERIODIC = "periodic"
LANE_SHED = "shed"

# A threading.Event cannot wake a Condition, so a blocked producer looks at its stop
# event this often. Short enough that shutdown feels immediate, and it costs nothing
# while the buffer has room.
_STOP_POLL_SECONDS = 0.05


class BufferCounts(NamedTuple):
    """One consistent reading of the buffer. Invariant: total + reserved <= limit."""

    online: int
    periodic: int
    shed: int
    reserved: int
    total: int


class StreamBuffer:
    """Stream buffer of PDF 4.1: three prioritised lanes sharing one capacity."""

    def __init__(
        self,
        limit: int = STREAM_BUFFER_LIMIT,
        online_reserve: int = ONLINE_RESERVE_SLOTS,
    ) -> None:
        if limit <= 0:
            raise ValueError("limit must be greater than zero")
        if not 0 <= online_reserve <= limit:
            raise ValueError("online_reserve must be between zero and the limit")
        self._limit = limit
        self._online_reserve = online_reserve
        self._online: deque[Transaction] = deque()
        self._periodic: deque[Transaction] = deque()
        self._shed: deque[Transaction] = deque()
        self._reserved = 0
        self._room_changed = threading.Condition(threading.Lock())

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def online_reserve(self) -> int:
        return self._online_reserve

    def put_online(
        self,
        items: Sequence[Transaction],
        stop_event: threading.Event | None = None,
        timeout: float | None = None,
    ) -> int:
        """Add online transactions, blocking while occupancy + reserved >= limit.

        Returns how many items from the front were accepted. That is fewer than
        len(items) only when stop_event was set or timeout ran out while the buffer was
        full. Nothing is ever dropped: the rest of the list still belongs to the caller.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        accepted = 0
        with self._room_changed:
            while accepted < len(items):
                room = self._limit - self._occupancy() - self._reserved
                if room > 0:
                    batch = items[accepted : accepted + room]
                    self._online.extend(batch)
                    accepted += len(batch)
                    continue
                if stop_event is not None and stop_event.is_set():
                    break
                wait_seconds = _STOP_POLL_SECONDS
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    wait_seconds = min(wait_seconds, remaining)
                self._room_changed.wait(wait_seconds)
        return accepted

    def reserve_slots(self, max_n: int, keep_free: int | None = None) -> int:
        """Reserve up to max_n slots for a later put_reserved and return how many.

        At most limit - occupancy - reserved - keep_free are granted, and never fewer
        than zero. keep_free defaults to the online reserve, so a reservation of any
        size still leaves the stream that many slots.
        """
        if max_n < 0:
            raise ValueError("max_n must not be negative")
        free = self._online_reserve if keep_free is None else keep_free
        if free < 0:
            raise ValueError("keep_free must not be negative")
        with self._room_changed:
            room = self._limit - self._occupancy() - self._reserved - free
            granted = max(0, min(max_n, room))
            self._reserved += granted
        return granted

    def put_reserved(
        self, lane: str, items: Sequence[Transaction], reserved_n: int
    ) -> int:
        """Turn a reservation of reserved_n slots into items on the periodic or shed lane.

        Returns the slots the items did not need, which go back to the free pool.
        Accepts nothing if the numbers are wrong, because a miscount here would quietly
        break the capacity invariant from then on.
        """
        if lane == LANE_PERIODIC:
            target = self._periodic
        elif lane == LANE_SHED:
            target = self._shed
        else:
            raise ValueError(f"lane must be {LANE_PERIODIC!r} or {LANE_SHED!r}")
        if len(items) > reserved_n:
            raise ValueError(f"{len(items)} items do not fit {reserved_n} reserved slots")
        unused = reserved_n - len(items)
        with self._room_changed:
            self._check_reserved(reserved_n)
            target.extend(items)
            self._reserved -= reserved_n
            if unused:
                self._room_changed.notify_all()
        return unused

    def release_slots(self, n: int) -> None:
        """Give back reserved slots that will not be used."""
        with self._room_changed:
            self._check_reserved(n)
            self._reserved -= n
            if n:
                self._room_changed.notify_all()

    def take(self, max_n: int = CHUNK_SIZE_W) -> list[Transaction]:
        """Remove up to max_n transactions: online first, then periodic, then shed.

        Never blocks. An empty result is a normal answer, because MESHJOIN must keep
        scanning Master Data R for the chunks already in memory.
        """
        if max_n < 0:
            raise ValueError("max_n must not be negative")
        taken: list[Transaction] = []
        with self._room_changed:
            for lane in (self._online, self._periodic, self._shed):
                while lane and len(taken) < max_n:
                    taken.append(lane.popleft())
            if taken:
                self._room_changed.notify_all()
        return taken

    def counts(self) -> BufferCounts:
        """Read every lane under one hold of the lock, so the numbers agree."""
        with self._room_changed:
            return BufferCounts(
                online=len(self._online),
                periodic=len(self._periodic),
                shed=len(self._shed),
                reserved=self._reserved,
                total=self._occupancy(),
            )

    def _occupancy(self) -> int:
        return len(self._online) + len(self._periodic) + len(self._shed)

    def _check_reserved(self, n: int) -> None:
        if not 0 <= n <= self._reserved:
            raise ValueError(f"{n} slots requested back but {self._reserved} are reserved")
