"""Constants, runtime settings and the command line.

PDF section 12 fixes the join parameters so that every submission can be checked
the same way. They are constants on purpose and must never become flags. Only the
values the PDF lets the user change (the arrival rates and the length of a
simulated hour) can be set from the command line.

There is no password flag and no password environment variable: the password is
typed at startup and lives only in ConnectionConfig.
"""

from __future__ import annotations

import argparse
import math
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

# Fixed by PDF section 12.
PARTITION_SIZE = 500
CHUNK_SIZE_W = 500
STREAM_BUFFER_LIMIT = 1000
PERIODIC_GROUPS = 48
DEFAULT_PEAK_TPS = 80
DEFAULT_NON_PEAK_TPS = 10
DEFAULT_SECONDS_PER_HOUR = 60.0

# Design choices that the PDF leaves open; the ReadMe explains each one.
ONLINE_RESERVE_SLOTS = 100
MIN_ITERATION_SECONDS = 0.2
HELD_PERIODIC_CAP = 20000
CACHE_FLUSH_SLICE = 2000
PERIODIC_LOAD_SLICE = 5000
TOPUP_INTERVAL_SECONDS = 0.2

SPEED_PEAK = "peak"
SPEED_NON_PEAK = "non-peak"
SPEEDS = (SPEED_PEAK, SPEED_NON_PEAK)

MODE_ONLINE = "online"
MODE_PERIODIC = "periodic"
MODE_BOTH = "both"
MODES = (MODE_ONLINE, MODE_PERIODIC, MODE_BOTH)

LIVE_LOAD_CACHE = "cache"
LIVE_LOAD_DIRECT = "direct"
LIVE_LOADS = (LIVE_LOAD_CACHE, LIVE_LOAD_DIRECT)

# Next to the package rather than the working directory, so the logs land in the
# same place wherever the program is started from.
DEFAULT_LOG_DIR = Path(__file__).resolve().parents[1] / "logs"


@dataclass(frozen=True)
class Settings:
    """Run settings the user may choose. Validated here so that every way of
    building them, not only the command line, gets the same rules."""

    peak_tps: int = DEFAULT_PEAK_TPS
    non_peak_tps: int = DEFAULT_NON_PEAK_TPS
    seconds_per_hour: float = DEFAULT_SECONDS_PER_HOUR
    start_speed: str = SPEED_PEAK
    start_mode: str = MODE_BOTH
    live_load: str = LIVE_LOAD_CACHE
    resume: bool = False
    log_dir: Path = DEFAULT_LOG_DIR

    def __post_init__(self) -> None:
        if self.peak_tps <= 0:
            raise ValueError("peak rate must be greater than zero")
        if self.non_peak_tps <= 0:
            raise ValueError("non-peak rate must be greater than zero")
        # A NaN fails every comparison and an infinite hour never ticks, so a
        # plain "> 0" would let both through.
        if not (math.isfinite(self.seconds_per_hour) and self.seconds_per_hour > 0):
            raise ValueError("seconds per hour must be a finite number greater than zero")
        if self.start_speed not in SPEEDS:
            raise ValueError(f"start speed must be one of {SPEEDS}")
        if self.start_mode not in MODES:
            raise ValueError(f"start mode must be one of {MODES}")
        if self.live_load not in LIVE_LOADS:
            raise ValueError(f"live load must be one of {LIVE_LOADS}")


@dataclass(frozen=True, repr=False)
class ConnectionConfig:
    """Where to connect. An empty database means no default database is selected.

    Written by hand instead of generated: the generated repr would print the
    password into any log line or traceback that shows this object.
    """

    host: str
    port: int
    database: str
    user: str
    password: str

    def __repr__(self) -> str:
        return (
            f"ConnectionConfig(host={self.host!r}, port={self.port!r}, "
            f"database={self.database!r}, user={self.user!r}, password='***')"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    """Create the parser. Abbreviations are off so that a typo such as --peak
    fails instead of silently meaning --peak-rate."""
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Near-real-time and periodic retail data warehouse using MESHJOIN.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--peak-rate",
        type=int,
        default=DEFAULT_PEAK_TPS,
        help="online transactions per second in peak time (default: %(default)s)",
    )
    parser.add_argument(
        "--non-peak-rate",
        type=int,
        default=DEFAULT_NON_PEAK_TPS,
        help="online transactions per second in non-peak time (default: %(default)s)",
    )
    parser.add_argument(
        "--seconds-per-hour",
        type=float,
        default=DEFAULT_SECONDS_PER_HOUR,
        help="real seconds in one simulated hour (default: %(default)s)",
    )
    parser.add_argument(
        "--start-speed",
        choices=SPEEDS,
        default=SPEED_PEAK,
        help="stream arrival mode at startup (default: %(default)s)",
    )
    parser.add_argument(
        "--start-mode",
        choices=MODES,
        default=MODE_BOTH,
        help="which forms of data run at startup (default: %(default)s)",
    )
    parser.add_argument(
        "--live-load",
        choices=LIVE_LOADS,
        default=LIVE_LOAD_CACHE,
        help="send Live results through the bonus cache or directly to the fact "
        "table (default: %(default)s)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="continue an interrupted run: keep the tables and skip data preparation",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=DEFAULT_LOG_DIR,
        help="directory for the log files (default: Mesh-Join/logs)",
    )
    return parser


def parse_settings(argv: Sequence[str] | None = None) -> Settings:
    """Read Settings from the command line, exiting with a usage message if invalid."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        return Settings(
            peak_tps=args.peak_rate,
            non_peak_tps=args.non_peak_rate,
            seconds_per_hour=args.seconds_per_hour,
            start_speed=args.start_speed,
            start_mode=args.start_mode,
            live_load=args.live_load,
            resume=args.resume,
            log_dir=args.log_dir,
        )
    except ValueError as exc:
        parser.error(str(exc))
