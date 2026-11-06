"""Write what a finished run proves to Project-Report/evidence/reference_run.json.

Every number comes from the warehouse database or the program's transcript, and the report
is built from this file, so the report cannot state anything the run did not measure.

    python tools/collect_run_evidence.py --database retail_dw --env-prefix DWH_LOCAL
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from retail_dw.db import Database  # noqa: E402
from tests.e2e.driver import connection_from_env  # noqa: E402

EVIDENCE = ROOT.parent / "Project-Report" / "evidence"


def _plain(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return value


def _rows(db: Database, sql: str) -> list[dict]:
    columns, rows = db.query_with_columns(sql)
    return [{c: _plain(v) for c, v in zip(columns, row, strict=True)} for row in rows]


def _one(db: Database, sql: str) -> dict:
    return _rows(db, sql)[0]


def collect(db: Database) -> dict:
    hours = _rows(
        db,
        "SELECT sim_hour, speed_mode, command_mode, group_no, group_size, admitted, shed_out, "
        "reinjected, periodic_loaded, shed_loaded, live_committed, on_shed_disk, on_live_cache, "
        "fact_rows, live_latency_p50_ms, live_latency_p95_ms, iterations, avg_iteration_ms "
        "FROM etl_hourly_log ORDER BY log_id",
    )
    released = [h for h in hours if h["group_no"] is not None]
    weighted = sum(h["iterations"] for h in hours) or 1
    totals = {
        "fact": _one(
            db,
            "SELECT COUNT(*) AS rows_loaded, SUM(quantity) AS quantity, "
            "CAST(SUM(revenue) AS CHAR) AS revenue, "
            "COUNT(DISTINCT transaction_id) AS carts FROM fact_sales",
        ),
        "by_route": _rows(
            db,
            "SELECT sales_mode, processing_mode, COUNT(*) AS rows_loaded, "
            "CAST(SUM(revenue) AS CHAR) AS revenue FROM fact_sales "
            "GROUP BY sales_mode, processing_mode ORDER BY 1, 2",
        ),
        "by_store": _rows(
            db,
            "SELECT s.store_id, s.store_name, COUNT(*) AS rows_loaded, "
            "COUNT(DISTINCT f.transaction_id) AS carts, "
            "CAST(SUM(f.revenue) AS CHAR) AS revenue FROM fact_sales f "
            "JOIN dim_store s ON s.store_key = f.store_key GROUP BY 1, 2 ORDER BY 1",
        ),
        "dimensions": _one(
            db,
            "SELECT (SELECT COUNT(*) FROM dim_customer) AS customers, "
            "(SELECT COUNT(*) FROM dim_product) AS products, "
            "(SELECT COUNT(*) FROM dim_supplier) AS suppliers, "
            "(SELECT COUNT(*) FROM dim_store) AS stores, "
            "(SELECT COUNT(*) FROM dim_time) AS time_rows",
        ),
        "left_over": _one(
            db,
            "SELECT (SELECT COUNT(*) FROM shed_table) AS shed, "
            "(SELECT COUNT(*) FROM live_cache) AS live_cache, "
            "(SELECT COUNT(*) FROM etl_reject) AS rejected, "
            "(SELECT COUNT(*) FROM periodic_sales_data WHERE released = 0) AS unreleased",
        ),
        "master_data": _one(
            db,
            "SELECT (SELECT COUNT(*) FROM customer_master_data) AS customers, "
            "(SELECT CEIL(COUNT(*) / 500) FROM customer_master_data) AS customer_partitions, "
            "(SELECT COUNT(*) FROM product_master_data) AS products, "
            "(SELECT CEIL(COUNT(*) / 500) FROM product_master_data) AS product_partitions",
        ),
        "groups": _one(
            db,
            "SELECT COUNT(*) AS groups_total, MIN(n) AS smallest, MAX(n) AS largest "
            "FROM (SELECT COUNT(*) AS n FROM periodic_sales_data GROUP BY release_hour) AS g",
        ),
    }
    skew = _one(
        db,
        "SELECT SUM(CASE WHEN r <= c * 0.1 THEN n END) / SUM(n) * 100 AS top_decile_share_pct "
        "FROM (SELECT COUNT(*) AS n, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS r, "
        "COUNT(*) OVER () AS c FROM fact_sales GROUP BY product_key) AS p",
    )
    customer_skew = _one(
        db,
        "SELECT SUM(CASE WHEN r <= c * 0.1 THEN n END) / SUM(n) * 100 AS top_decile_share_pct "
        "FROM (SELECT COUNT(*) AS n, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS r, "
        "COUNT(*) OVER () AS c FROM fact_sales GROUP BY customer_key) AS p",
    )
    spikes = _spikes(db)
    return {
        "skew": {"products": skew, "customers": customer_skew},
        "q19": spikes,
        "server_version": db.query("SELECT VERSION()")[0][0],
        "hours": hours,
        "groups_released": len(released),
        "shed_by_speed": {
            speed: {
                "hours": sum(1 for h in hours if h["speed_mode"] == speed),
                "shed_out": sum(h["shed_out"] for h in hours if h["speed_mode"] == speed),
                "returned": sum(h["reinjected"] for h in hours if h["speed_mode"] == speed),
                "shed_rows_loaded": sum(h["shed_loaded"] for h in hours if h["speed_mode"] == speed),
            }
            for speed in ("peak", "non-peak")
        },
        "peak_hour_shed_range": _shed_range(released),
        "iteration_ms_weighted_average": round(
            sum(h["avg_iteration_ms"] * h["iterations"] for h in hours) / weighted, 2
        ),
        "live_latency_ms": {
            "p50_of_hours_median": _median([h["live_latency_p50_ms"] for h in hours]),
            "p95_of_hours_median": _median([h["live_latency_p95_ms"] for h in hours]),
            "p95_worst_hour": max(
                (h["live_latency_p95_ms"] or 0 for h in hours), default=None
            ),
        },
        **totals,
    }


def _spikes(db: Database) -> dict:
    daily = (
        "WITH d AS (SELECT f.product_key, t.full_date, SUM(f.revenue) AS rev "
        "FROM fact_sales f JOIN dim_time t ON t.time_key = f.time_key "
        "GROUP BY f.product_key, t.full_date), "
        "s AS (SELECT product_key, full_date, rev, SUM(rev) OVER (PARTITION BY product_key) AS tot, "
        "COUNT(*) OVER (PARTITION BY product_key) AS days FROM d) "
    )
    summary = _one(
        db,
        daily + "SELECT COUNT(*) AS spike_days, COUNT(DISTINCT product_key) AS products, "
        "(SELECT COUNT(*) FROM s) AS product_days FROM s WHERE rev * days > 2 * tot",
    )
    largest = _rows(
        db,
        daily + "SELECT p.product_id, p.product_name, CAST(s.full_date AS CHAR) AS day, s.rev AS revenue, "
        "ROUND(s.tot / s.days, 2) AS average, ROUND(s.rev * s.days / s.tot, 1) AS times "
        "FROM s JOIN dim_product p ON p.product_key = s.product_key "
        "WHERE s.rev * s.days > 2 * s.tot ORDER BY times DESC LIMIT 5",
    )
    by_month = _rows(
        db,
        daily + "SELECT DATE_FORMAT(full_date, '%Y-%m') AS month, COUNT(*) AS spike_days "
        "FROM s WHERE rev * days > 2 * tot GROUP BY 1 ORDER BY 1",
    )
    return {**summary, "largest": largest, "by_month": by_month}


def _shed_range(released: list[dict]) -> dict | None:
    shed = [h["shed_out"] for h in released if h["speed_mode"] == "peak" and h["shed_out"]]
    return {"min": min(shed), "max": max(shed), "hours": len(shed)} if shed else None


def _median(values: list) -> float | None:
    present = sorted(v for v in values if v is not None)
    return present[len(present) // 2] if present else None


def from_transcript(path: Path) -> dict:
    """The program's own printed numbers, and how long the run took."""
    if not path.is_file():
        return {}
    text = path.read_text(encoding="utf-8")
    found: dict = {"statuses": text.count(">>> status")}
    for key, pattern in (
        ("before_cleaning", r"Transactions before cleaning: ([\d,]+)"),
        ("after_cleaning", r"Transactions after cleaning: ([\d,]+)"),
    ):
        match = re.search(pattern, text)
        if match:
            found[key] = int(match.group(1).replace(",", ""))
    stamps = re.findall(r"^\s*([\d.]+)s  ", text, re.M)
    if stamps:
        found["elapsed_seconds"] = float(stamps[-1])
    loaded = re.search(r"^\s*([\d.]+)s  ALL DATA LOADED", text, re.M)
    if loaded:
        found["all_loaded_at_seconds"] = float(loaded.group(1))
    return found


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--env-prefix", default="DWH_TEST")
    parser.add_argument("--transcript", default=str(EVIDENCE / "reference_run_transcript.txt"))
    args = parser.parse_args()
    with Database(connection_from_env(args.env_prefix, args.database)) as db:
        evidence = collect(db)
    evidence["transcript"] = from_transcript(Path(args.transcript))
    evidence["collected_at"] = datetime.now().isoformat(timespec="seconds")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / "reference_run.json").write_text(json.dumps(evidence, indent=1), encoding="utf-8")
    print(f"Wrote {EVIDENCE / 'reference_run.json'}")
    print("fact:", evidence["fact"], "left over:", evidence["left_over"])
    print("shed by speed:", evidence["shed_by_speed"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
