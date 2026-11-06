"""The optional dashboard against the OLAP database: every endpoint, and its safety rules."""

from __future__ import annotations

import http.client
import json
import logging
import os
import threading
from dataclasses import replace

import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.dashboard.queries import load_queries, load_titles
from retail_dw.dashboard.server import MAX_DISPLAY_ROWS, Warehouse, make_server
from retail_dw.db import Database, MySqlError, locate_create_dw_script, run_sql_script

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def dashboard_cfg(temp_database: ConnectionConfig) -> ConnectionConfig:
    """A small schema with a few facts. DWH_OLAP_DB points at a fully loaded one instead."""
    override = os.environ.get("DWH_OLAP_DB")
    if override:
        return replace(temp_database, database=override)
    with Database(temp_database) as db:
        run_sql_script(db, locate_create_dw_script())
        db.execute("INSERT INTO dim_customer VALUES (1,'1','Male','0-17','Student','A','0','Single')")
        db.execute("INSERT INTO dim_product VALUES (1,'P','Name','Toys')")
        db.execute("INSERT INTO dim_supplier VALUES (1,'SUP01','Northwind Supplies')")
        db.execute("INSERT INTO dim_store VALUES (1,1,'Online','Web')")
        db.execute(
            "INSERT INTO dim_time VALUES (201101031000,'2011-01-03 10:00:00','2011-01-03',10,3,1,"
            "'January',1,2011,1,'Monday','Weekday','Winter','H1')"
        )
        db.execute(
            "INSERT INTO fact_sales (customer_key, product_key, supplier_key, store_key, time_key,"
            " transaction_id, sales_mode, processing_mode, quantity, revenue) VALUES "
            "(1,1,1,1,201101031000,'T1','Online','Live',2,2.500)"
        )
    return temp_database


@pytest.fixture(scope="module")
def server(dashboard_cfg: ConnectionConfig):
    instance = make_server(dashboard_cfg, 0)
    thread = threading.Thread(target=instance.serve_forever, daemon=True)
    thread.start()
    yield instance
    instance.shutdown()
    instance.server_close()


def get(server, path: str, host: str = "127.0.0.1") -> tuple[int, str]:
    connection = http.client.HTTPConnection("127.0.0.1", server.server_address[1], timeout=170)
    connection.request("GET", path, headers={"Host": f"{host}:{server.server_address[1]}"})
    response = connection.getresponse()
    return response.status, response.read().decode("utf-8")


def test_binds_to_loopback_only(server) -> None:
    assert server.server_address[0] == "127.0.0.1"


def test_page_is_self_contained(server) -> None:
    status, body = get(server, "/")
    assert status == 200
    assert "Retail Warehouse" in body
    assert "https://" not in body and "http://" not in body


@pytest.mark.parametrize("path", ["/api/live", "/api/hourly", "/api/kpis", "/api/queries"])
def test_data_endpoints_return_json(server, path: str) -> None:
    status, body = get(server, path)
    assert status == 200
    assert isinstance(json.loads(body), dict)


def test_kpis_are_real_numbers(server) -> None:
    kpis = json.loads(get(server, "/api/kpis")[1])
    assert kpis["rows"] > 0 and kpis["revenue"] > 0
    assert kpis["months"] and kpis["stores"]


def test_all_twenty_questions_are_listed_and_ddl_is_not(server) -> None:
    ids = [q["id"] for q in json.loads(get(server, "/api/queries")[1])["queries"]]
    assert ids == [f"Q{n}" for n in range(1, 21)]
    assert get(server, "/api/olap/Q20_view")[0] == 404


def test_query_returns_the_submitted_sql(server) -> None:
    answer = json.loads(get(server, "/api/olap/Q2")[1])
    assert answer["sql"] == load_queries()["Q2"]
    assert answer["columns"][:2] == ["gender", "age_group"]
    assert answer["shown_rows"] <= MAX_DISPLAY_ROWS


@pytest.mark.parametrize(
    "path",
    [
        "/api/olap/Q99",
        "/api/olap/Q1;DROP%20TABLE%20fact_sales",
        "/api/olap/..%2f..%2fetc",
        "/api/olap/",
        "/secret",
        "/../main.py",
    ],
)
def test_unknown_ids_and_paths_are_refused(server, path: str) -> None:
    assert get(server, path)[0] == 404


def test_other_hosts_are_refused_against_dns_rebinding(server) -> None:
    assert get(server, "/api/kpis", host="evil.example.com")[0] == 403
    assert get(server, "/", host="evil.example.com")[0] == 403


def test_sessions_cannot_write(dashboard_cfg: ConnectionConfig) -> None:
    warehouse = Warehouse(dashboard_cfg)
    with warehouse._session() as db:
        with pytest.raises(MySqlError) as caught:
            db.execute("DELETE FROM fact_sales")
    assert caught.value.errno == 1792


def test_no_password_in_any_response_or_log(
    server, dashboard_cfg: ConnectionConfig, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    for path in ("/", "/api/live", "/api/hourly", "/api/kpis", "/api/queries", "/api/olap/Q99"):
        assert dashboard_cfg.password not in get(server, path)[1]
    assert dashboard_cfg.password not in caplog.text


def test_missing_view_gives_a_helpful_message_not_internals(
    server, dashboard_cfg: ConnectionConfig
) -> None:
    if os.environ.get("DWH_OLAP_DB"):
        pytest.skip("the loaded warehouse already has the view")
    status, body = get(server, "/api/olap/Q20")
    assert status == 503
    assert "Queries-DW/queries_dw.sql" in json.loads(body)["error"]
    assert "SELECT" not in body


def test_titles_cover_every_question() -> None:
    assert set(load_titles()) >= {f"Q{n}" for n in range(1, 21)}
