"""A read-only web view of the warehouse. A separate process, not part of the ETL threads.

It listens on 127.0.0.1 only, each session is set to read-only by the server, and the only
SQL it can run is fixed here or comes from the graded query script by id. Nothing from a
request ever becomes SQL.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from retail_dw.config import ConnectionConfig
from retail_dw.dashboard.queries import load_queries, load_titles
from retail_dw.db import Database, MySqlError

logger = logging.getLogger(__name__)

INDEX_HTML = Path(__file__).with_name("static") / "index.html"
MAX_DISPLAY_ROWS = 500
ALLOWED_HOSTS = {"127.0.0.1", "localhost"}

_MONTHLY = (
    "SELECT t.year_no, t.month_no, MIN(t.month_name), SUM(f.revenue), COUNT(*) "
    "FROM fact_sales f JOIN dim_time t ON t.time_key = f.time_key "
    "GROUP BY t.year_no, t.month_no ORDER BY t.year_no, t.month_no"
)
_STORES = (
    "SELECT s.store_name, SUM(f.revenue), COUNT(*), COUNT(DISTINCT f.transaction_id) "
    "FROM fact_sales f JOIN dim_store s ON s.store_key = f.store_key "
    "GROUP BY s.store_name ORDER BY s.store_name"
)
_KPIS = (
    "SELECT COUNT(*), COALESCE(SUM(revenue), 0), COALESCE(SUM(quantity), 0), "
    "COUNT(DISTINCT transaction_id), COUNT(DISTINCT customer_key), "
    "COUNT(DISTINCT product_key), COALESCE(SUM(CASE WHEN sales_mode = 'Online' "
    "THEN revenue END), 0) FROM fact_sales"
)
_HOURLY_COLUMNS = (
    "sim_hour",
    "speed_mode",
    "command_mode",
    "group_no",
    "group_size",
    "admitted",
    "shed_out",
    "reinjected",
    "periodic_loaded",
    "shed_loaded",
    "live_committed",
    "on_shed_disk",
    "on_live_cache",
    "fact_rows",
    "live_latency_p50_ms",
    "live_latency_p95_ms",
    "iterations",
    "avg_iteration_ms",
)
_STATUS_COLUMNS = (
    "command_mode",
    "speed_mode",
    "current_tps",
    "sim_hour",
    "stream_buffer",
    "stream_online",
    "stream_periodic",
    "stream_shed",
    "hash_table",
    "queue_chunks",
    "customer_partition",
    "customer_partitions",
    "product_partition",
    "product_partitions",
    "on_shed_disk",
    "on_live_cache",
    "held_periodic",
    "fact_rows",
    "live_load",
    "state",
    "updated_at",
)


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Cannot send a {type(value).__name__}")


class Warehouse:
    """All the database reads. One short read-only session per request."""

    def __init__(self, cfg: ConnectionConfig) -> None:
        self._cfg = cfg
        self.queries = load_queries()
        self.titles = load_titles()

    def _session(self) -> Database:
        db = Database(self._cfg)
        db.execute("SET SESSION TRANSACTION READ ONLY")
        return db

    def live(self) -> dict[str, Any]:
        with self._session() as db:
            status = db.query(
                f"SELECT {', '.join(_STATUS_COLUMNS)}, "
                "TIMESTAMPDIFF(SECOND, updated_at, NOW(3)) FROM etl_live_status WHERE id = 1"
            )
            waiting = db.query(
                "SELECT COALESCE(SUM(released = 0), 0), COUNT(*) FROM periodic_sales_data"
            )[0]
            return {
                "status": dict(zip(_STATUS_COLUMNS, status[0], strict=False)) if status else None,
                "age_seconds": status[0][-1] if status else None,
                "fact": db.query("SELECT COUNT(*) FROM fact_sales")[0][0],
                "shed": db.query("SELECT COUNT(*) FROM shed_table")[0][0],
                "cache": db.query("SELECT COUNT(*) FROM live_cache")[0][0],
                "rejected": db.query("SELECT COUNT(*) FROM etl_reject")[0][0],
                "staging_waiting": waiting[0],
                "staging_total": waiting[1],
            }

    def hourly(self) -> dict[str, Any]:
        with self._session() as db:
            rows = db.query(
                f"SELECT {', '.join(_HOURLY_COLUMNS)} FROM etl_hourly_log ORDER BY log_id"
            )
        return {"rows": [dict(zip(_HOURLY_COLUMNS, row, strict=True)) for row in rows]}

    def kpis(self) -> dict[str, Any]:
        with self._session() as db:
            rows, revenue, quantity, carts, customers, products, online = db.query(_KPIS)[0]
            months = db.query(_MONTHLY)
            stores = db.query(_STORES)
        return {
            "rows": rows,
            "revenue": revenue,
            "quantity": quantity,
            "carts": carts,
            "customers": customers,
            "products": products,
            "online_revenue": online,
            "months": [
                {"label": f"{m[2][:3]} {m[0]}", "revenue": m[3], "rows": m[4]} for m in months
            ],
            "stores": [
                {"store": s[0], "revenue": s[1], "rows": s[2], "carts": s[3]} for s in stores
            ],
        }

    def query_list(self) -> dict[str, Any]:
        return {
            "queries": [
                {"id": query_id, "title": self.titles.get(query_id, query_id)}
                for query_id in self.queries
            ]
        }

    def olap(self, query_id: str) -> dict[str, Any] | None:
        sql = self.queries.get(query_id)
        if sql is None:
            return None
        with self._session() as db:
            columns, rows = db.query_with_columns(sql)
        return {
            "id": query_id,
            "title": self.titles.get(query_id, query_id),
            "sql": sql,
            "columns": columns,
            "total_rows": len(rows),
            "shown_rows": min(len(rows), MAX_DISPLAY_ROWS),
            "rows": rows[:MAX_DISPLAY_ROWS],
        }


def make_handler(warehouse: Warehouse) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "RetailDW"
        sys_version = ""

        def do_GET(self) -> None:  # noqa: N802 - the name http.server expects
            host = self.headers.get("Host", "").rsplit(":", 1)[0]
            if host not in ALLOWED_HOSTS:
                # Stops a web page on another site reaching this server by DNS rebinding.
                self._send_json(403, {"error": "This dashboard is for local use only."})
                return
            path = self.path.split("?", 1)[0]
            if path == "/":
                self._send(200, "text/html; charset=utf-8", INDEX_HTML.read_bytes())
                return
            routes = {
                "/api/live": warehouse.live,
                "/api/hourly": warehouse.hourly,
                "/api/kpis": warehouse.kpis,
                "/api/queries": warehouse.query_list,
            }
            try:
                if path in routes:
                    self._send_json(200, routes[path]())
                elif path.startswith("/api/olap/"):
                    answer = warehouse.olap(path.removeprefix("/api/olap/"))
                    if answer is None:
                        self._send_json(404, {"error": "No such query."})
                    else:
                        self._send_json(200, answer)
                else:
                    self._send_json(404, {"error": "Not found."})
            except MySqlError as exc:
                logger.warning("Dashboard query failed with MySQL error %s", exc.errno)
                self._send_json(503, {"error": _friendly(exc.errno)})

        def _send_json(self, code: int, body: dict[str, Any]) -> None:
            data = json.dumps(body, default=_json_value).encode("utf-8")
            self._send(code, "application/json", data)

        def _send(self, code: int, content_type: str, data: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            logger.debug("%s", format % args)

    return Handler


def _friendly(errno: int) -> str:
    if errno == 1146:
        return (
            "A table or view is missing. Run the program to load the warehouse, and "
            "Queries-DW/queries_dw.sql to create STORE_QUARTERLY_SALES."
        )
    return f"The database could not answer (MySQL error {errno})."


def make_server(cfg: ConnectionConfig, port: int) -> ThreadingHTTPServer:
    """Bound to the loopback address only. Port 0 picks a free port, for tests."""
    return ThreadingHTTPServer(("127.0.0.1", port), make_handler(Warehouse(cfg)))
