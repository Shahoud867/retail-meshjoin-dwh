"""Run state shared by the console and the three worker threads.

The console changes this state and the threads read it on every loop, so a typed
command takes effect at once. No thread ever waits for another thread's turn here:
each method holds the lock only long enough to read or change a value.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

from retail_dw.config import (
    MODE_BOTH,
    MODE_ONLINE,
    MODE_PERIODIC,
    MODES,
    PERIODIC_GROUPS,
    SPEED_NON_PEAK,
    SPEED_PEAK,
    SPEEDS,
    Settings,
)


class RuntimeControl:
    """Mode, speed, simulated hour and stop signals for one run."""

    def __init__(self, settings: Settings, sim_hour: int = 0, next_group: int = 0) -> None:
        self._lock = threading.Lock()
        self._mode = settings.start_mode
        self._speed = settings.start_speed
        self._peak_tps = settings.peak_tps
        self._non_peak_tps = settings.non_peak_tps
        self._sim_hour = sim_hour
        self._next_group = next_group
        self._fatal_error: BaseException | None = None
        self._fatal_listener: Callable[[BaseException], object] | None = None

        # Readers stop first so that the join can empty what is already in memory.
        self.stop_readers = threading.Event()
        # Everything stops at once. Set on a fatal error, or when the join has drained.
        self.stop_all = threading.Event()
        self.online_done = threading.Event()
        self.periodic_done = threading.Event()

    @property
    def mode(self) -> str:
        with self._lock:
            return self._mode

    @property
    def speed(self) -> str:
        with self._lock:
            return self._speed

    @property
    def peak_tps(self) -> int:
        with self._lock:
            return self._peak_tps

    @property
    def non_peak_tps(self) -> int:
        with self._lock:
            return self._non_peak_tps

    def current_tps(self) -> int:
        """The online arrival rate in force right now."""
        with self._lock:
            return self._peak_tps if self._speed == SPEED_PEAK else self._non_peak_tps

    def set_mode(self, mode: str) -> None:
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        with self._lock:
            self._mode = mode

    def set_speed(self, speed: str, tps: int | None = None) -> None:
        """Switch speed, and optionally change that speed's rate as well."""
        if speed not in SPEEDS:
            raise ValueError(f"speed must be one of {SPEEDS}")
        if tps is not None and tps <= 0:
            raise ValueError("rate must be greater than zero")
        with self._lock:
            self._speed = speed
            if tps is not None:
                if speed == SPEED_PEAK:
                    self._peak_tps = tps
                else:
                    self._non_peak_tps = tps

    def online_enabled(self) -> bool:
        """The stream reader runs in `online` and `both`; `periodic` pauses it."""
        with self._lock:
            return self._mode in (MODE_ONLINE, MODE_BOTH)

    def periodic_enabled(self) -> bool:
        """Periodic groups are extracted in `periodic` and `both`; `online` stops them."""
        with self._lock:
            return self._mode in (MODE_PERIODIC, MODE_BOTH)

    def shed_allowed(self) -> bool:
        """Periodic rows may be moved to the shed disk only at peak speed."""
        with self._lock:
            return self._speed == SPEED_PEAK

    def shed_processing_allowed(self) -> bool:
        """Shed rows return at non-peak speed. `online` means online form only, so
        they stay on disk in that mode."""
        with self._lock:
            return self._speed == SPEED_NON_PEAK and self._mode in (MODE_PERIODIC, MODE_BOTH)

    def cache_flush_allowed(self) -> bool:
        """The bonus cache empties in a slow period, which this project reads as non-peak."""
        with self._lock:
            return self._speed == SPEED_NON_PEAK

    @property
    def sim_hour(self) -> int:
        with self._lock:
            return self._sim_hour

    def advance_hour(self) -> int:
        """Move the simulated clock one hour on and return the new hour."""
        with self._lock:
            self._sim_hour += 1
            return self._sim_hour

    @property
    def next_group(self) -> int:
        """The next periodic group not yet opened, from 0 to PERIODIC_GROUPS."""
        with self._lock:
            return self._next_group

    @property
    def opened_group(self) -> int:
        """The highest group opened so far, or -1 before the first one."""
        with self._lock:
            return self._next_group - 1

    def open_next_group(self) -> int | None:
        """Open the next group and return its number, or None when none are left.
        Groups therefore open strictly in order and never twice."""
        with self._lock:
            if self._next_group >= PERIODIC_GROUPS:
                return None
            group = self._next_group
            self._next_group += 1
            return group

    def fatal(self, error: BaseException) -> None:
        """Record the first fatal error and stop everything. Later errors are usually
        consequences of the first one, so the first is the one worth reporting."""
        with self._lock:
            first = self._fatal_error is None
            if first:
                self._fatal_error = error
            listener = self._fatal_listener
        self.stop_readers.set()
        self.stop_all.set()
        if first and listener is not None:
            listener(error)

    def set_fatal_listener(self, listener: Callable[[BaseException], object]) -> None:
        """Tell the console at once, since it may be blocked waiting for typed input."""
        with self._lock:
            self._fatal_listener = listener

    @property
    def fatal_error(self) -> BaseException | None:
        with self._lock:
            return self._fatal_error
