"""The real program, as a child process, on a small dataset: every mode, and every failure.

These tests type at the program's stdin and read its stdout, so they exercise exactly what
a person does, including the three threads, MySQL, and the file the program writes.
"""

from __future__ import annotations

import os
import re
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.db import Database, ensure_database, run_sql_script, locate_create_dw_script
from tests.e2e.driver import Program

pytestmark = [pytest.mark.integration, pytest.mark.slow]

ROWS = 6000
HOUR = 1.0


@pytest.fixture(scope="module")
def raw_file(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A small file in the shape of the real one, with bad rows the cleaning must remove."""
    start = datetime(2010, 12, 1, 8, 0)
    rows = []
    for i in range(ROWS):
        cart = 500000 + i // 8
        rows.append(
            {
                "InvoiceNo": cart,
                "StockCode": f"{10000 + i % 900}{'A' if i % 7 == 0 else ''}",
                "Description": f"PRODUCT {i % 900}",
                "Quantity": 1 + i % 5,
                "InvoiceDate": start + timedelta(minutes=i // 8),
                "UnitPrice": 1.25 + (i % 11) / 100,
                "CustomerID": 12000 + i % 1100,
                "Country": "United Kingdom",
            }
        )
    rows += [
        {**rows[0], "InvoiceNo": "C999999"},
        {**rows[0], "Quantity": -1},
        {**rows[0], "UnitPrice": 0.0},
        {**rows[0], "CustomerID": None},
    ]
    path = tmp_path_factory.mktemp("raw") / "small.xlsx"
    pd.DataFrame(rows).to_excel(path, index=False)
    return path


@pytest.fixture
def run_db(db_cfg: ConnectionConfig, request: pytest.FixtureRequest):
    name = "t_e2e_" + re.sub(r"\W", "_", request.node.name.lower())[:40]
    cfg = replace(db_cfg, database=name)
    with Database(db_cfg) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS `{name}`")
    yield cfg
    with Database(db_cfg) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS `{name}`")


_started: list[Program] = []


@pytest.fixture(autouse=True)
def _no_program_outlives_its_test():
    """A failed assertion must not leave a program running into the next test."""
    yield
    while _started:
        program = _started.pop()
        if program.poll() is None:
            program.kill()


def start(cfg, raw_file: Path, tmp_path: Path, *flags: str, online: str = "online.csv") -> Program:
    log_dir = tmp_path / "logs"
    program = Program(
        str(tmp_path / "transcript.txt"),
        cfg,
        [str(tmp_path / online), str(raw_file)],
        ["--seconds-per-hour", str(HOUR), "--log-dir", str(log_dir), *flags],
    )
    _started.append(program)
    return program


def wait_until(condition, timeout: float = 90.0, message: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.2)
    raise AssertionError(f"Timed out waiting for {message}")


def scalar(cfg, sql: str):
    with Database(cfg) as db:
        return db.query(sql)[0][0]


def fact_lines(cfg) -> Counter:
    """Every fact row in natural terms, so that a lost or doubled row shows."""
    with Database(cfg) as db:
        return Counter(
            db.query(
                "SELECT f.transaction_id, p.product_id, c.customer_id, f.quantity, f.revenue, "
                "t.full_datetime FROM fact_sales f "
                "JOIN dim_product p ON p.product_key = f.product_key "
                "JOIN dim_customer c ON c.customer_key = f.customer_key "
                "JOIN dim_time t ON t.time_key = f.time_key"
            )
        )


def expected_lines(raw_file: Path) -> Counter:
    frame = pd.read_excel(raw_file, dtype=object)
    frame = frame[
        ~frame["InvoiceNo"].astype(str).str.startswith("C")
        & (pd.to_numeric(frame["Quantity"]) > 0)
        & (pd.to_numeric(frame["UnitPrice"]) > 0)
        & frame["CustomerID"].notna()
    ]
    lines: Counter = Counter()
    for row in frame.itertuples():
        revenue = Decimal(int(row.Quantity)) * Decimal(repr(float(row.UnitPrice)))
        lines[
            (
                str(row.InvoiceNo),
                str(row.StockCode),
                str(int(row.CustomerID)),
                int(row.Quantity),
                revenue.quantize(Decimal("0.001")),
                pd.Timestamp(row.InvoiceDate).to_pydatetime(),
            )
        ] += 1
    return lines


def assert_complete(cfg, raw_file: Path) -> None:
    assert fact_lines(cfg) == expected_lines(raw_file)
    assert scalar(cfg, "SELECT COUNT(*) FROM shed_table") == 0
    assert scalar(cfg, "SELECT COUNT(*) FROM live_cache") == 0
    assert scalar(cfg, "SELECT COUNT(*) FROM etl_reject") == 0
    assert scalar(cfg, "SELECT COUNT(*) FROM periodic_sales_data WHERE released = 0") == 0
    assert scalar(cfg, "SELECT COUNT(*) FROM periodic_sales_data WHERE loaded <> 1") == 0


def read_output(program: Program) -> str:
    return "".join(program.lines)


class TestRequiredOutput:
    def test_banner_prompts_status_and_clean_exit(self, run_db, raw_file, tmp_path):
        program = start(run_db, raw_file, tmp_path, "--peak-rate", "300")
        assert program.wait_for("Threads: OnlineThread", 120)
        program.send("status")
        assert program.wait_for("Shed disk:", 10)
        program.send("not-a-command")
        assert program.wait_for("Unknown command", 10)
        program.send("peak 0")
        assert program.wait_for("The rate must be", 10)
        program.send("quit")
        assert program.wait_exit(60) == 0
        text = read_output(program)
        for required in (
            "Host",
            "Port",
            "Database name",
            "User name",
            "Password",
            "Path of the online file",
            "Transactions before cleaning: 6,004",
            "Transactions after cleaning: 6,000",
            "Peak time: 300 transactions per second",
            "Non-peak time: 10 transactions per second",
            "Stream arrival mode: peak",
            "One simulated hour: 1 seconds",
            "Stream buffer limit: 1,000 transactions",
            "Master Data R: customer 3 partitions, product",
            "Periodic groups: 48",
            "Live load: cache",
            "Mode: both | Speed: peak (300 tps)",
            "Hash table H:",
            "Queue:",
            "Disk buffer: customer partition",
            "Row accounting:",
            "Balanced",
        ):
            assert required in text, required
        assert text.index("Stream buffer limit") < text.index("Type a command")


class TestModes:
    def test_full_run_with_shedding_then_non_peak(self, run_db, raw_file, tmp_path):
        """Peak sheds only periodic rows, and non-peak brings them all back."""
        program = start(run_db, raw_file, tmp_path, "--peak-rate", "60", "--non-peak-rate", "2000")
        assert program.wait_for("Threads: OnlineThread", 120)
        # This small file's groups fit the buffer, so little is shed here. TestShedding uses
        # groups too big for the buffer. This test proves the cache and the final totals.
        wait_until(lambda: scalar(run_db, "SELECT COUNT(*) FROM live_cache") > 0, message="cache")
        assert (
            scalar(run_db, "SELECT COUNT(*) FROM fact_sales WHERE processing_mode = 'Live'") == 0
        ), "Live rows reached the fact table during peak while the cache was on"
        program.send("non-peak")
        assert program.wait_for("ALL DATA LOADED", 180), read_output(program)[-2000:]
        program.send("quit")
        assert program.wait_exit(60) == 0
        assert_complete(run_db, raw_file)
        assert "Complete: every transaction" in read_output(program)

    def test_direct_load_never_uses_the_cache(self, run_db, raw_file, tmp_path):
        program = start(
            run_db, raw_file, tmp_path, "--live-load", "direct", "--peak-rate", "3000"
        )
        assert program.wait_for("Live load: direct", 120)
        wait_until(
            lambda: scalar(run_db, "SELECT COUNT(*) FROM fact_sales WHERE processing_mode='Live'")
            > 0,
            message="Live rows in the fact table",
        )
        assert scalar(run_db, "SELECT COUNT(*) FROM live_cache") == 0
        # 3,000 tps fills the buffer, so a group can be shed at peak. Those rows need non-peak.
        program.send("non-peak 3000")
        assert program.wait_for("ALL DATA LOADED", 180), read_output(program)[-2000:]
        assert scalar(run_db, "SELECT COUNT(*) FROM live_cache") == 0
        program.send("quit")
        assert program.wait_exit(60) == 0
        assert_complete(run_db, raw_file)

    def test_online_mode_extracts_no_groups(self, run_db, raw_file, tmp_path):
        program = start(run_db, raw_file, tmp_path, "--start-mode", "online", "--peak-rate", "500")
        assert program.wait_for("Threads: OnlineThread", 120), read_output(program)[-1500:]
        wait_until(lambda: scalar(run_db, "SELECT COUNT(*) FROM live_cache") > 100, message="cache")
        time.sleep(3 * HOUR)
        assert scalar(run_db, "SELECT COUNT(*) FROM periodic_sales_data WHERE released = 1") == 0
        program.send("status")
        assert program.wait_for("Groups released: 0/48", 10)
        program.send("quit")
        assert program.wait_exit(60) == 0

    def test_periodic_mode_pauses_the_stream_and_loads_on_the_tick(
        self, run_db, raw_file, tmp_path
    ):
        program = start(
            run_db, raw_file, tmp_path, "--start-mode", "periodic", "--seconds-per-hour", "3"
        )
        assert program.wait_for("Threads: OnlineThread", 120)
        wait_until(
            lambda: scalar(run_db, "SELECT COUNT(*) FROM periodic_sales_data WHERE released = 1")
            > 0,
            message="first group",
        )
        # Group 0 joins in about 2 seconds and must then wait for the hour to end.
        wait_until(
            lambda: scalar(run_db, "SELECT COUNT(*) FROM fact_sales") > 0,
            message="the first tick",
        )
        program.send("status")
        time.sleep(6)
        assert scalar(run_db, "SELECT COUNT(*) FROM live_cache") == 0
        assert (
            scalar(run_db, "SELECT COUNT(*) FROM fact_sales WHERE processing_mode = 'Live'") == 0
        )
        with Database(run_db) as db:
            moments = db.query(
                "SELECT COUNT(DISTINCT FLOOR(UNIX_TIMESTAMP(loaded_at))) FROM fact_sales"
            )[0][0]
            groups_seen = db.query(
                "SELECT COUNT(DISTINCT release_hour) FROM periodic_sales_data WHERE loaded = 1"
            )[0][0]
        # Rows land in bursts on ticks, about one burst per hour, not as they join.
        assert moments <= groups_seen + 2
        program.send("quit")
        assert program.wait_exit(60) == 0


@pytest.fixture(scope="module")
def burst_raw_file(tmp_path_factory: pytest.TempPathFactory, raw_file: Path) -> Path:
    """Periodic groups too big for the buffer, as in the real data."""
    frame = pd.read_excel(raw_file, dtype=object)
    frame = frame[frame["InvoiceNo"].astype(str).str.isdigit()].copy()
    copies = []
    # Enough rows that 30% over 48 groups exceeds the 1,000-slot buffer.
    for copy in range(36):
        extra = frame.copy()
        extra["InvoiceNo"] = extra["InvoiceNo"].astype(int) + copy * 100000
        copies.append(extra)
    path = tmp_path_factory.mktemp("burst") / "burst.xlsx"
    pd.concat(copies).to_excel(path, index=False)
    return path


class TestShedding:
    def test_peak_sheds_periodic_only_and_non_peak_returns_them(
        self, run_db, burst_raw_file, tmp_path
    ):
        program = start(run_db, burst_raw_file, tmp_path, "--peak-rate", "50", "--seconds-per-hour", "2")
        assert program.wait_for("Threads: OnlineThread", 300)
        wait_until(lambda: scalar(run_db, "SELECT COUNT(*) FROM shed_table") > 0, message="shed")
        with Database(run_db) as db:
            modes = db.query("SELECT DISTINCT sales_mode FROM shed_table")
            on_disk = db.query("SELECT COUNT(*) FROM shed_table")[0][0]
        assert modes == [("Periodic",)]

        # At peak the shed disk only grows: nothing returns from it.
        time.sleep(3)
        assert scalar(run_db, "SELECT COUNT(*) FROM shed_table") >= on_disk
        assert scalar(run_db, "SELECT COUNT(*) FROM fact_sales WHERE processing_mode='Shed'") == 0

        # `online` is online form only, so even at non-peak the shed rows must stay on disk.
        program.send("online")
        program.send("non-peak")
        time.sleep(2)
        kept = scalar(run_db, "SELECT COUNT(*) FROM shed_table")
        time.sleep(4)
        assert scalar(run_db, "SELECT COUNT(*) FROM shed_table") == kept

        # `both` at non-peak returns them, and stops creating new ones.
        program.send("non-peak 20000")
        program.send("both")
        wait_until(
            lambda: scalar(run_db, "SELECT COUNT(*) FROM shed_table") == 0,
            timeout=300,
            message="the shed disk to empty",
        )
        assert program.wait_for("ALL DATA LOADED", 300), read_output(program)[-2000:]
        program.send("quit")
        assert program.wait_exit(120) == 0
        assert scalar(run_db, "SELECT COUNT(*) FROM fact_sales WHERE processing_mode = 'Shed'") > 0
        assert_complete(run_db, burst_raw_file)
        with Database(run_db) as db:
            total_shed = db.query("SELECT COALESCE(SUM(shed_out), 0) FROM etl_hourly_log")[0][0]
            shed_in_fact = db.query(
                "SELECT COUNT(*) FROM fact_sales WHERE processing_mode = 'Shed'"
            )[0][0]
        assert total_shed == shed_in_fact


class TestFailureAndRecovery:
    def test_process_killed_mid_run_resumes_exactly_once(self, run_db, burst_raw_file, tmp_path):
        """No row lost and none doubled, across a hard kill at each stage of the data."""
        program = start(run_db, burst_raw_file, tmp_path, "--peak-rate", "4000", "--seconds-per-hour", "2")
        assert program.wait_for("Threads: OnlineThread", 300)
        wait_until(
            lambda: scalar(run_db, "SELECT COUNT(*) FROM shed_table") > 0
            and scalar(run_db, "SELECT COUNT(*) FROM live_cache") > 5000,
            timeout=120,
            message="both disks to hold rows",
        )
        program.kill()

        before = scalar(
            run_db,
            "SELECT online_rows_committed FROM etl_progress WHERE writer = 'meshjoin'",
        )
        assert before > 0
        second = start(run_db, burst_raw_file, tmp_path, "--resume", "--peak-rate", "4000", "--seconds-per-hour", "2")
        assert second.wait_for("Resuming:", 60), read_output(second)[-1500:]
        assert second.wait_for("Threads: OnlineThread", 60)
        time.sleep(4)
        second.kill()

        third = start(run_db, burst_raw_file, tmp_path, "--resume", "--start-speed", "non-peak", "--non-peak-rate", "20000", "--seconds-per-hour", "2")
        assert third.wait_for("ALL DATA LOADED", 400), read_output(third)[-2000:]
        third.send("quit")
        assert third.wait_exit(120) == 0
        assert_complete(run_db, burst_raw_file)
        assert "Balanced" in read_output(third)

    def test_killed_database_connections_lose_nothing(self, run_db, db_cfg, raw_file, tmp_path):
        program = start(run_db, raw_file, tmp_path, "--peak-rate", "800", "--live-load", "direct")
        assert program.wait_for("Threads: OnlineThread", 120)
        kills = 0
        with Database(db_cfg) as admin:
            for _ in range(4):
                time.sleep(1.5)
                sessions = admin.query(
                    "SELECT id FROM information_schema.processlist WHERE db = %s",
                    (run_db.database,),
                )
                for (session,) in sessions:
                    try:
                        admin.execute(f"KILL {int(session)}")
                        kills += 1
                    except Exception:  # noqa: BLE001 - the session may already have ended
                        pass
        assert kills > 0
        assert program.wait_for("ALL DATA LOADED", 180), read_output(program)[-2000:]
        program.send("quit")
        assert program.wait_exit(60) == 0
        assert_complete(run_db, raw_file)

    def test_fatal_error_stops_cleanly_and_then_resumes(self, run_db, raw_file, tmp_path):
        """Master Data disappearing mid-run must stop the run, not corrupt the warehouse."""
        program = start(run_db, raw_file, tmp_path, "--peak-rate", "200")
        assert program.wait_for("Threads: OnlineThread", 120)
        wait_until(lambda: scalar(run_db, "SELECT COUNT(*) FROM live_cache") > 0, message="cache")
        with Database(run_db) as db:
            saved = db.query("SELECT * FROM customer_master_data")
            db.execute("DELETE FROM customer_master_data")
        assert program.wait_for("ERROR: the run is stopping", 30), read_output(program)[-1500:]
        program.send("")
        assert program.wait_exit(60) == 1
        assert "--resume" in read_output(program)

        with Database(run_db) as db:
            db.executemany(
                "INSERT INTO customer_master_data VALUES (%s,%s,%s,%s,%s,%s,%s)", saved
            )
        second = start(run_db, raw_file, tmp_path, "--resume", "--start-speed", "non-peak", "--non-peak-rate", "5000")
        assert second.wait_for("ALL DATA LOADED", 180), read_output(second)[-2000:]
        second.send("quit")
        assert second.wait_exit(60) == 0
        assert_complete(run_db, raw_file)

    def test_resume_refuses_an_unprepared_database(self, run_db, raw_file, tmp_path):
        ensure_database(run_db)
        online = tmp_path / "online.csv"
        online.write_text("x\n", encoding="utf-8")
        program = start(run_db, raw_file, tmp_path, "--resume")
        assert program.wait_exit(60) == 1
        assert "no finished data preparation" in read_output(program)


class TestStartupSafety:
    def test_wrong_password_exits_without_echoing_it(self, run_db, raw_file, tmp_path):
        wrong = replace(run_db, password="not-the-real-secret-xyz")
        program = start(wrong, raw_file, tmp_path)
        assert program.wait_exit(60) == 1
        time.sleep(0.5)
        text = read_output(program)
        assert "Access denied" in text
        assert wrong.password not in text
        for log in (tmp_path / "logs").glob("*.log"):
            assert wrong.password not in log.read_text(encoding="utf-8")

    def test_password_never_reaches_output_or_logs(self, run_db, raw_file, tmp_path):
        program = start(run_db, raw_file, tmp_path, "--peak-rate", "500")
        assert program.wait_for("Threads: OnlineThread", 120)
        program.send("quit")
        assert program.wait_exit(60) == 0
        time.sleep(0.5)
        assert run_db.password not in read_output(program)
        logs = list((tmp_path / "logs").glob("*.log"))
        assert logs
        for log in logs:
            assert run_db.password not in log.read_text(encoding="utf-8")

    def test_foreign_tables_are_not_dropped_without_a_person(self, run_db, raw_file, tmp_path):
        ensure_database(run_db)
        with Database(run_db) as db:
            db.execute("CREATE TABLE fact_sales (other_project INT)")
            db.execute("INSERT INTO fact_sales VALUES (42)")
        program = start(run_db, raw_file, tmp_path)
        assert program.wait_exit(60) == 1
        assert "was not created by this program" in read_output(program)
        assert scalar(run_db, "SELECT other_project FROM fact_sales") == 42

    def test_unrelated_tables_survive_a_normal_start(self, run_db, raw_file, tmp_path):
        ensure_database(run_db)
        with Database(run_db) as db:
            db.execute("CREATE TABLE my_own_table (keep_me INT)")
            db.execute("INSERT INTO my_own_table VALUES (7)")
        program = start(run_db, raw_file, tmp_path, "--peak-rate", "500")
        assert program.wait_for("Threads: OnlineThread", 120)
        program.send("quit")
        assert program.wait_exit(60) == 0
        assert scalar(run_db, "SELECT keep_me FROM my_own_table") == 7

    def test_a_second_start_rebuilds_everything(self, run_db, raw_file, tmp_path):
        first = start(run_db, raw_file, tmp_path, "--peak-rate", "3000", "--non-peak-rate", "3000")
        assert first.wait_for("Threads: OnlineThread", 120)
        time.sleep(3)
        first.send("quit")
        assert first.wait_exit(60) == 0
        with Database(run_db) as db:
            db.execute("CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS SELECT 1 AS x")
        second = start(run_db, raw_file, tmp_path, "--start-speed", "non-peak", "--non-peak-rate", "5000")
        assert second.wait_for("ALL DATA LOADED", 180), read_output(second)[-2000:]
        second.send("quit")
        assert second.wait_exit(60) == 0
        assert_complete(run_db, raw_file)
        assert scalar(
            run_db,
            "SELECT COUNT(*) FROM information_schema.views WHERE table_schema = DATABASE()",
        ) == 0


class TestScriptsRunStandalone:
    def test_create_script_is_what_the_program_runs(self, run_db):
        ensure_database(run_db)
        with Database(run_db) as db:
            run_sql_script(db, locate_create_dw_script())
            run_sql_script(db, locate_create_dw_script())
            assert db.query("SELECT COUNT(*) FROM fact_sales") == [(0,)]


def test_no_environment_secret_in_source_tree() -> None:
    password = os.environ.get("DWH_TEST_PASSWORD")
    if not password:
        pytest.skip("no password to look for")
    root = Path(__file__).resolve().parents[3]
    for path in root.rglob("*"):
        if not path.is_file() or ".venv" in path.parts or ".git" in path.parts:
            continue
        if path.suffix in {".xlsx", ".pdf", ".pyc", ".docx", ".png"} or path.stat().st_size > 5e6:
            continue
        assert password not in path.read_text(encoding="utf-8", errors="ignore"), path
