"""Drive the real program through its real stdin and stdout, exactly as a person would.

Connection details come from environment variables named by --env-prefix, so nothing secret
is ever on a command line or in a file. Output is copied to a transcript as it arrives.

    python tests/e2e/driver.py --database retail_dw --env-prefix DWH_LOCAL
        --transcript ../Project-Report/evidence/reference_run_transcript.txt
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from retail_dw.config import ConnectionConfig  # noqa: E402
from retail_dw.db import Database  # noqa: E402

COMPLETE_MARK = "ALL DATA LOADED"


class Program:
    """main.py as a child process. Its output is watched from a helper thread."""

    def __init__(self, transcript: str, cfg: ConnectionConfig, paths: list[str], extra: list[str]):
        self.lines: list[str] = []
        self._transcript = open(transcript, "a", encoding="utf-8")
        self.started = time.monotonic()
        self._process = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "main.py"), *extra],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            cwd=ROOT,
        )
        threading.Thread(target=self._pump, daemon=True).start()
        for answer in (cfg.host, str(cfg.port), cfg.database, cfg.user, cfg.password, *paths):
            self._process.stdin.write(answer + "\n")
        self._process.stdin.flush()

    def _pump(self) -> None:
        for line in self._process.stdout:
            self.lines.append(line)
            self._transcript.write(f"{time.monotonic() - self.started:9.1f}s  {line}")
            self._transcript.flush()

    def send(self, command: str) -> None:
        self._transcript.write(f"{time.monotonic() - self.started:9.1f}s  >>> {command}\n")
        self._process.stdin.write(command + "\n")
        self._process.stdin.flush()

    def seen(self, text: str) -> bool:
        return any(text in line for line in self.lines)

    def wait_for(self, text: str, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.seen(text):
                return True
            if self._process.poll() is not None:
                return False
            time.sleep(0.5)
        return False

    def elapsed(self) -> float:
        return time.monotonic() - self.started

    def wait_exit(self, timeout: float) -> int:
        return self._process.wait(timeout)

    def kill(self) -> None:
        self._process.kill()
        self._process.wait()

    def poll(self) -> int | None:
        return self._process.poll()


def online_finished(cfg: ConnectionConfig, online_rows: int) -> bool:
    with Database(cfg) as db:
        rows = db.query("SELECT online_rows_committed FROM etl_progress WHERE writer = 'meshjoin'")
    return bool(rows) and rows[0][0] >= online_rows


def run_schedule(
    program: Program, cfg: ConnectionConfig, online_rows: int, peak_s: float, slow_s: float
) -> None:
    """The reference schedule: peak with shedding, then non-peak so the shed rows and the
    cache drain, then peak for the remaining online rows, then a last slow period."""
    if not program.wait_for("Threads: OnlineThread", 900):
        raise SystemExit("The program did not reach the threads")
    begin = program.elapsed()
    program.send("status")
    next_status = begin + 300
    phases = [(begin + peak_s, "non-peak"), (begin + peak_s + slow_s, "peak")]
    finishing = False
    while program.poll() is None:
        now = program.elapsed()
        if phases and now >= phases[0][0]:
            program.send(phases.pop(0)[1])
            program.send("status")
        if now >= next_status:
            program.send("status")
            next_status = now + 300
        if not phases and not finishing and online_finished(cfg, online_rows):
            finishing = True
            program.send("status")
            program.send("non-peak")
        if program.seen(COMPLETE_MARK):
            time.sleep(2)
            program.send("status")
            time.sleep(2)
            program.send("quit")
            return
        time.sleep(1)
    raise SystemExit(f"The program exited early with code {program.poll()}")


def keep_computer_awake() -> None:
    """Ask Windows not to sleep while this process runs. It changes no setting, and ends
    with the process. A sleeping laptop would otherwise distort a timed run."""
    if sys.platform == "win32":
        import ctypes

        es_continuous, es_system_required = 0x80000000, 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(es_continuous | es_system_required)


def connection_from_env(prefix: str, database: str) -> ConnectionConfig:
    return ConnectionConfig(
        os.environ[f"{prefix}_HOST"],
        int(os.environ[f"{prefix}_PORT"]),
        database,
        os.environ[f"{prefix}_USER"],
        os.environ[f"{prefix}_PASSWORD"],
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--env-prefix", default="DWH_TEST")
    parser.add_argument("--transcript", required=True)
    parser.add_argument(
        "--online-file", default=str(ROOT.parent / "Data" / "online_sales_stream.csv")
    )
    parser.add_argument(
        "--raw-file", default=str(ROOT.parent / "Data" / "raw" / "Online Retail.xlsx")
    )
    parser.add_argument("--online-rows", type=int, default=278740)
    parser.add_argument("--peak-seconds", type=float, default=1800)
    parser.add_argument("--slow-seconds", type=float, default=1080)
    parser.add_argument("program_args", nargs="*")
    args = parser.parse_args()
    keep_computer_awake()
    cfg = connection_from_env(args.env_prefix, args.database)
    Path(args.transcript).parent.mkdir(parents=True, exist_ok=True)
    program = Program(args.transcript, cfg, [args.online_file, args.raw_file], args.program_args)
    run_schedule(program, cfg, args.online_rows, args.peak_seconds, args.slow_seconds)
    code = program.wait_exit(600)
    print(f"program exit code {code}, elapsed {program.elapsed():.0f}s")
    return code


if __name__ == "__main__":
    sys.exit(main())
