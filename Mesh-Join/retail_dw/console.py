"""What the user sees and types: startup prompts, the banner, commands and status."""

from __future__ import annotations

import getpass
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TextIO

from retail_dw.config import (
    MODES,
    PERIODIC_GROUPS,
    SPEED_NON_PEAK,
    SPEED_PEAK,
    STREAM_BUFFER_LIMIT,
    LIVE_LOAD_CACHE,
    ConnectionConfig,
    Settings,
)
from retail_dw.control import RuntimeControl
from retail_dw.db import validate_database_name
from retail_dw.threads.meshjoin_thread import PipelineStatus

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ONLINE_PATH = _PROJECT_ROOT / "Data" / "online_sales_stream.csv"
DEFAULT_RAW_PATH = _PROJECT_ROOT / "Data" / "raw" / "Online Retail.xlsx"
DEFAULT_DATABASE = "retail_dw"

HELP_TEXT = """Commands:
  online          online form only; do not extract periodic groups
  periodic        periodic groups only; pause the stream reader
  both            both forms; Live transactions are taken first
  peak [tps]      fast speed; shedding periodic transactions to disk is allowed
  non-peak [tps]  slow speed; processing shed transactions is allowed
  status          print the mode, speed, Stream buffer, Hash table H and shed disk
  help            print this list
  quit            stop the threads and close the program"""


class StartupAborted(RuntimeError):
    """Input ended before the startup questions were answered."""


def _ask(prompt: str, default: str | None, stdin: TextIO, out: TextIO) -> str:
    suffix = f" [{default}]" if default else ""
    out.write(f"{prompt}{suffix}: ")
    out.flush()
    line = stdin.readline()
    if not line:
        raise StartupAborted("Input ended before the startup questions were answered")
    return line.strip() or (default or "")


def read_password(prompt: str = "Password: ", stdin: TextIO | None = None) -> str:
    """Never echoed. On a terminal the input is hidden. Without one (a test harness) one
    line is read from the pipe, which nothing displays."""
    stream = sys.stdin if stdin is None else stdin
    if stream.isatty():
        return getpass.getpass(prompt)
    sys.stdout.write(prompt)
    sys.stdout.flush()
    line = stream.readline()
    if not line:
        raise StartupAborted("Input ended before the password was given")
    return line.rstrip("\r\n")


def prompt_connection(
    stdin: TextIO | None = None, out: TextIO | None = None
) -> ConnectionConfig:
    """Ask for the connection details (PDF section 6). The password goes nowhere else."""
    stream_in = sys.stdin if stdin is None else stdin
    stream_out = sys.stdout if out is None else out
    host = _ask("Host", "localhost", stream_in, stream_out)
    while True:
        port_text = _ask("Port", "3306", stream_in, stream_out)
        if port_text.isdigit() and 0 < int(port_text) < 65536:
            break
        stream_out.write("The port must be a number from 1 to 65535.\n")
    while True:
        database = _ask("Database name", DEFAULT_DATABASE, stream_in, stream_out)
        try:
            validate_database_name(database)
            break
        except ValueError:
            stream_out.write("Use only letters, digits and underscores, at most 64.\n")
    user = _ask("User name", "root", stream_in, stream_out)
    password = read_password(stdin=stream_in)
    return ConnectionConfig(host, int(port_text), database, user, password)


def prompt_paths(
    stdin: TextIO | None = None, out: TextIO | None = None
) -> tuple[Path, Path]:
    """Ask for the path of the online file, and of the raw file it is prepared from."""
    stream_in = sys.stdin if stdin is None else stdin
    stream_out = sys.stdout if out is None else out
    online = _ask("Path of the online file", str(DEFAULT_ONLINE_PATH), stream_in, stream_out)
    raw = _ask("Path of Online Retail.xlsx", str(DEFAULT_RAW_PATH), stream_in, stream_out)
    return Path(online), Path(raw)


def print_banner(
    settings: Settings,
    control: RuntimeControl,
    customer_partitions: int,
    product_partitions: int,
    group_min: int | None,
    group_max: int | None,
    out: Callable[[str], object] = print,
) -> None:
    """Printed before the threads start (PDF sections 5.2, 6 and 12)."""
    live = (
        "cache (bonus: Live results wait in live_cache for a slow period)"
        if settings.live_load == LIVE_LOAD_CACHE
        else "direct (Live results go straight to the fact table)"
    )
    out(f"Peak time: {control.peak_tps} transactions per second")
    out(f"Non-peak time: {control.non_peak_tps} transactions per second")
    out(f"Stream arrival mode: {control.speed}")
    out(f"One simulated hour: {settings.seconds_per_hour:g} seconds")
    out(f"Stream buffer limit: {STREAM_BUFFER_LIMIT:,} transactions")
    out(
        f"Master Data R: customer {customer_partitions} partitions, "
        f"product {product_partitions} partitions"
    )
    if group_min is not None:
        out(f"Periodic groups: {PERIODIC_GROUPS}, smallest {group_min:,}, largest {group_max:,}")
    out(f"Live load: {live}")
    out(f"Starting mode: {control.mode}")
    out("Threads: OnlineThread, MeshJoinThread, PeriodicThread")


def format_status(status: PipelineStatus) -> str:
    """The status text, in the component names of PDF section 4.1."""
    return "\n".join(
        (
            f"Mode: {status.mode} | Speed: {status.speed} ({status.tps} tps) | "
            f"Simulated hour: {status.sim_hour} | "
            f"Groups released: {status.groups_released}/{PERIODIC_GROUPS}",
            f"Stream buffer: {status.stream_buffer}/{status.stream_limit} "
            f"(online {status.stream_online}, periodic {status.stream_periodic}, "
            f"shed {status.stream_shed})",
            f"Hash table H: {status.hash_table} transactions",
            f"Queue: {status.queue_chunks} chunks",
            f"Disk buffer: customer partition {status.customer_partition}/"
            f"{status.customer_partitions}, product partition {status.product_partition}/"
            f"{status.product_partitions}",
            f"Shed disk: {status.on_shed_disk} transactions",
            f"Live cache: {status.on_live_cache} | "
            f"Periodic results held for the tick: {status.held_periodic}",
            f"Fact: {status.fact_rows}",
        )
    )


def run_command(
    line: str,
    control: RuntimeControl,
    status_text: Callable[[], str],
    out: Callable[[str], object] = print,
) -> bool:
    """Carry out one typed command. Returns False when the program should close."""
    words = line.strip().lower().split()
    if not words:
        return True
    command, arguments = words[0], words[1:]
    if command == "quit" and not arguments:
        return False
    if command == "status" and not arguments:
        out(status_text())
    elif command in MODES and not arguments:
        control.set_mode(command)
        out(f"Mode is now {command}.")
    elif command in (SPEED_PEAK, SPEED_NON_PEAK) and len(arguments) <= 1:
        _run_speed_command(command, arguments, control, out)
    elif command == "help" and not arguments:
        out(HELP_TEXT)
    else:
        out(f"Unknown command: {line.strip()}")
        out(HELP_TEXT)
    return True


def _run_speed_command(
    speed: str,
    arguments: list[str],
    control: RuntimeControl,
    out: Callable[[str], object],
) -> None:
    tps: int | None = None
    if arguments:
        if not arguments[0].isdigit() or int(arguments[0]) <= 0:
            out(f"The rate must be a whole number above zero, not {arguments[0]}.")
            return
        tps = int(arguments[0])
    control.set_speed(speed, tps)
    out(f"Speed is now {speed} ({control.current_tps()} tps).")


def command_loop(
    control: RuntimeControl,
    status_text: Callable[[], str],
    stdin: TextIO | None = None,
    out: Callable[[str], object] = print,
) -> None:
    """Keep accepting commands until quit, end of input or Ctrl+C (PDF section 9 step 5)."""
    stream = sys.stdin if stdin is None else stdin
    out("Type a command, or help for the list.")
    try:
        while True:
            line = stream.readline()
            if not line:
                return
            if control.fatal_error is not None:
                return
            if not run_command(line, control, status_text, out):
                return
    except KeyboardInterrupt:
        out("Stopping.")
