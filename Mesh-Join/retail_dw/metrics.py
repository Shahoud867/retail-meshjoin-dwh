"""Thread-safe counters behind `status`, the hourly log and the report's shed counts.

Every counter has a run total and an hourly value. Reading the hour resets only the
hourly values, so the totals always cover the whole run.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

# Counter names. A misspelt name raises KeyError, so a typo cannot quietly start a
# second counter that nothing ever reads.
ONLINE_READ = "online_read"
ONLINE_BAD_LINES = "online_bad_lines"
GROUP_SIZE = "group_size"
ADMITTED = "admitted"
SHED_OUT = "shed_out"
REINJECTED = "reinjected"
PERIODIC_LOADED = "periodic_loaded"
SHED_LOADED = "shed_loaded"
LIVE_COMMITTED = "live_committed"
LIVE_LOADED = "live_loaded"
REJECTED = "rejected"
ITERATIONS = "iterations"

_COUNTERS = (
    ONLINE_READ,
    ONLINE_BAD_LINES,
    GROUP_SIZE,
    ADMITTED,
    SHED_OUT,
    REINJECTED,
    PERIODIC_LOADED,
    SHED_LOADED,
    LIVE_COMMITTED,
    LIVE_LOADED,
    REJECTED,
    ITERATIONS,
)

# Enough samples for stable percentiles in one hour. At 80 tps an hour of 60 seconds
# is 4,800 rows, so keeping a bounded sample costs almost nothing in accuracy.
_MAX_LATENCY_SAMPLES = 20000


@dataclass(frozen=True)
class HourStats:
    """What happened during one simulated hour."""

    counters: dict[str, int]
    live_latency_p50_ms: int | None
    live_latency_p95_ms: int | None
    avg_iteration_ms: float

    def __getitem__(self, name: str) -> int:
        return self.counters[name]


def _percentile_ms(sorted_seconds: list[float], fraction: float) -> int | None:
    if not sorted_seconds:
        return None
    index = min(len(sorted_seconds) - 1, int(fraction * len(sorted_seconds)))
    return round(sorted_seconds[index] * 1000)


class Metrics:
    """Counters shared by all threads. Each call holds the lock only briefly."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._totals = dict.fromkeys(_COUNTERS, 0)
        self._hour = dict.fromkeys(_COUNTERS, 0)
        self._latencies: list[float] = []
        self._iteration_seconds = 0.0

    def add(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._totals[name] += amount
            self._hour[name] += amount

    def total(self, name: str) -> int:
        with self._lock:
            return self._totals[name]

    def totals(self) -> dict[str, int]:
        with self._lock:
            return dict(self._totals)

    def record_iteration(self, seconds: float) -> None:
        with self._lock:
            self._totals[ITERATIONS] += 1
            self._hour[ITERATIONS] += 1
            self._iteration_seconds += seconds

    def record_live_latency(self, seconds: list[float]) -> None:
        """Record arrival-to-commit times of Live rows for the near-real-time evidence."""
        with self._lock:
            room = _MAX_LATENCY_SAMPLES - len(self._latencies)
            if room > 0:
                self._latencies.extend(seconds[:room])

    def take_hour(self) -> HourStats:
        """Return the hour that has just ended and start counting the next one."""
        with self._lock:
            counters = dict(self._hour)
            latencies = sorted(self._latencies)
            iterations = counters[ITERATIONS]
            average_ms = (self._iteration_seconds / iterations * 1000) if iterations else 0.0
            self._hour = dict.fromkeys(_COUNTERS, 0)
            self._latencies = []
            self._iteration_seconds = 0.0
        return HourStats(
            counters=counters,
            live_latency_p50_ms=_percentile_ms(latencies, 0.50),
            live_latency_p95_ms=_percentile_ms(latencies, 0.95),
            avg_iteration_ms=average_ms,
        )
