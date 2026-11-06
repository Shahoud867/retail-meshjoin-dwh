"""The schema script and the Database wrapper, against a real MySQL server.

Connection-loss tests use real failures. A KILL covers the session dying before
COMMIT, and a small TCP relay covers the cases a KILL cannot produce on demand: a
COMMIT that lands while its reply is lost, and a COMMIT that lands late.
"""

from __future__ import annotations

import logging
import select
import socket
import threading
import time
import traceback
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import mysql.connector
import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.db import (
    _BUMP_TXN_SEQ,
    CommitOutcomeUnknownError,
    Database,
    MySqlError,
    ensure_database,
    locate_create_dw_script,
    run_sql_script,
    split_sql_statements,
)

pytestmark = pytest.mark.integration

ER_ROW_IS_REFERENCED = 1451
ER_NO_REFERENCED_ROW = 1452
ER_CHECK_VIOLATED = 3819
ER_DUPLICATE_ENTRY = 1062
ER_DATA_TOO_LONG = 1406
ER_ACCESS_DENIED = 1045
ER_NO_SUCH_TABLE = 1146
LOST_CONNECTION = {2006, 2013, 2055}

DIM_CUSTOMER = """
    customer_key int unsigned, customer_id varchar(12), gender varchar(6),
    age_group varchar(5), occupation varchar(12), city_category char(1),
    stay_in_current_city_years varchar(2), marital_status varchar(7)"""
DIM_PRODUCT = """
    product_key int unsigned, product_id varchar(20), product_name varchar(255),
    product_category varchar(20)"""
FACT_MEASURES = """
    transaction_id varchar(10), sales_mode varchar(8), processing_mode varchar(8),
    quantity int, revenue decimal(14,3), loaded_at datetime(3)"""
CUSTOMER_ATTRIBUTES = """
    gender varchar(6), age_group varchar(5), occupation varchar(12),
    city_category char(1), stay_in_current_city_years varchar(2),
    marital_status varchar(7)"""
TRANSACTION_COLUMNS = """
    transaction_id varchar(10), customer_id varchar(12), product_id varchar(20),
    quantity int, unit_price decimal(10,3), event_time datetime, sales_mode varchar(8)"""

# The contract from BUILD_SPEC section 5, as columns in order with their exact types.
EXPECTED_COLUMNS = {
    "dim_customer": DIM_CUSTOMER,
    "dim_product": DIM_PRODUCT,
    "dim_supplier": """
        supplier_key smallint unsigned, supplier_id varchar(8), supplier_name varchar(40)""",
    "dim_store": """
        store_key smallint unsigned, store_id tinyint unsigned, store_name varchar(40),
        city varchar(30)""",
    "dim_time": """
        time_key bigint unsigned, full_datetime datetime, full_date date, hour_of_day tinyint,
        day_of_month tinyint, month_no tinyint, month_name varchar(9), quarter_no tinyint,
        year_no smallint, day_of_week tinyint, day_name varchar(9), day_type varchar(7),
        season varchar(6), half_year char(2)""",
    "fact_sales": f"""
        sales_key bigint unsigned, customer_key int unsigned, product_key int unsigned,
        supplier_key smallint unsigned, store_key smallint unsigned, time_key bigint unsigned,
        {FACT_MEASURES}""",
    "customer_master_data": f"customer_id varchar(12), {CUSTOMER_ATTRIBUTES}",
    "product_master_data": """
        product_id varchar(20), product_name varchar(255), product_category varchar(20),
        supplier_id varchar(8), supplier_name varchar(40)""",
    "periodic_sales_data": f"""
        staging_id int unsigned, {TRANSACTION_COLUMNS}, release_hour smallint,
        released tinyint, loaded tinyint""",
    "shed_table": f"""
        shed_id bigint unsigned, staging_id int unsigned, {TRANSACTION_COLUMNS},
        shed_time datetime(3)""",
    "live_cache": f"""
        cache_id bigint unsigned, transaction_id varchar(10), customer_id varchar(12),
        {CUSTOMER_ATTRIBUTES}, product_id varchar(20), product_name varchar(255),
        product_category varchar(20), supplier_id varchar(8), supplier_name varchar(40),
        store_id tinyint unsigned, event_time datetime, quantity int, revenue decimal(14,3),
        sales_mode varchar(8), processing_mode varchar(8), cached_at datetime(3)""",
    "etl_hourly_log": """
        log_id int unsigned, sim_hour int, speed_mode varchar(8), command_mode varchar(8),
        group_no int, group_size int, admitted int, shed_out int, reinjected int,
        periodic_loaded int, shed_loaded int, live_committed int, on_shed_disk int,
        on_live_cache int, fact_rows bigint, live_latency_p50_ms int, live_latency_p95_ms int,
        iterations int, avg_iteration_ms decimal(8,2), logged_at datetime(3)""",
    "etl_reject": f"""
        reject_id bigint unsigned, {TRANSACTION_COLUMNS}, processing_mode varchar(8),
        reason varchar(40), rejected_at datetime(3)""",
    "etl_progress": """
        writer varchar(16), txn_seq bigint, online_rows_committed bigint, next_group int,
        sim_hour int, prepared tinyint, updated_at datetime(3)""",
    "etl_live_status": """
        id tinyint, command_mode varchar(8), speed_mode varchar(8), current_tps int, sim_hour int,
        stream_buffer int, stream_online int, stream_periodic int, stream_shed int, hash_table int,
        queue_chunks int, customer_partition int, customer_partitions int, product_partition int,
        product_partitions int, on_shed_disk int, on_live_cache int, held_periodic int,
        fact_rows bigint, live_load varchar(6), state varchar(16), updated_at datetime(3)""",
}

EXPECTED_PRIMARY_KEYS = {
    "dim_customer": "customer_key",
    "dim_product": "product_key",
    "dim_supplier": "supplier_key",
    "dim_store": "store_key",
    "dim_time": "time_key",
    "fact_sales": "sales_key",
    "customer_master_data": "customer_id",
    "product_master_data": "product_id",
    "periodic_sales_data": "staging_id",
    "shed_table": "shed_id",
    "live_cache": "cache_id",
    "etl_hourly_log": "log_id",
    "etl_reject": "reject_id",
    "etl_progress": "writer",
    "etl_live_status": "id",
}

EXPECTED_FOREIGN_KEYS = {
    ("fact_sales", "customer_key", "dim_customer", "customer_key"),
    ("fact_sales", "product_key", "dim_product", "product_key"),
    ("fact_sales", "supplier_key", "dim_supplier", "supplier_key"),
    ("fact_sales", "store_key", "dim_store", "store_key"),
    ("fact_sales", "time_key", "dim_time", "time_key"),
}

EXPECTED_UNIQUE_KEYS = {
    ("dim_customer", "customer_id"),
    ("dim_product", "product_id"),
    ("dim_supplier", "supplier_id"),
    ("dim_store", "store_id"),
    ("dim_time", "full_datetime"),
    ("shed_table", "staging_id"),
}


def _columns_of(spec: str) -> list[tuple[str, str]]:
    """Parse the 'name type, name type' text above. Commas inside decimal(14,3) are
    protected by looking only at commas followed by a space or newline."""
    parts = [part.strip() for part in spec.replace(",\n", ", ").split(", ")]
    return [tuple(part.split(None, 1)) for part in parts]


def _kill(cfg: ConnectionConfig, connection_id: int) -> None:
    with Database(cfg) as admin:
        admin.execute(f"KILL {int(connection_id)}")


def _txn_seq(db: Database, writer: str) -> int:
    rows = db.query("SELECT txn_seq FROM etl_progress WHERE writer = %s", (writer,))
    return rows[0][0] if rows else 0


def _reject_count(db: Database, reason: str) -> int:
    rows = db.query("SELECT COUNT(*) FROM etl_reject WHERE reason = %s", (reason,))
    return rows[0][0]


def _insert_reject(db: Database, reason: str) -> int:
    """A cheap marker row, to count how many times a transaction's work took effect."""
    return db.execute("INSERT INTO etl_reject (reason) VALUES (%s)", (reason,))


def _seed_dimensions(db: Database) -> dict[str, int]:
    """Insert one row into every dimension and return a valid set of fact keys."""
    db.execute(
        "INSERT INTO dim_customer (customer_id, gender, age_group, occupation, city_category,"
        " stay_in_current_city_years, marital_status)"
        " VALUES ('17850', 'Male', '18-25', 'Clerk', 'A', '4+', 'Single')"
    )
    db.execute(
        "INSERT INTO dim_product (product_id, product_name, product_category)"
        " VALUES ('85123A', 'HEART HOLDER', 'Toys')"
    )
    db.execute(
        "INSERT INTO dim_supplier (supplier_id, supplier_name) VALUES ('SUP01', 'Northwind Supplies')"
    )
    db.execute(
        "INSERT INTO dim_store (store_id, store_name, city) VALUES (1, 'Online', 'Web')"
    )
    db.execute(
        "INSERT INTO dim_time (time_key, full_datetime, full_date, hour_of_day, day_of_month,"
        " month_no, month_name, quarter_no, year_no, day_of_week, day_name, day_type, season,"
        " half_year) VALUES (201012010826, '2010-12-01 08:26:00', '2010-12-01', 8, 1, 12,"
        " 'December', 4, 2010, 3, 'Wednesday', 'Weekday', 'Winter', 'H2')"
    )
    return {
        "customer_key": db.query("SELECT customer_key FROM dim_customer")[0][0],
        "product_key": db.query("SELECT product_key FROM dim_product")[0][0],
        "supplier_key": db.query("SELECT supplier_key FROM dim_supplier")[0][0],
        "store_key": db.query("SELECT store_key FROM dim_store")[0][0],
        "time_key": 201012010826,
    }


def _insert_fact(db: Database, keys: dict[str, int], **overrides: Any) -> int:
    row = {
        **keys,
        "transaction_id": "536365",
        "sales_mode": "Online",
        "processing_mode": "Live",
        "quantity": 6,
        "revenue": Decimal("15.300"),
        **overrides,
    }
    columns = ", ".join(row)
    placeholders = ", ".join(["%s"] * len(row))
    return db.execute(
        f"INSERT INTO fact_sales ({columns}) VALUES ({placeholders})", tuple(row.values())
    )


# --------------------------------------------------------------------------- script


class TestSchemaScript:
    def test_runs_twice_with_only_notes(self, db: Database) -> None:
        statements = split_sql_statements(
            locate_create_dw_script().read_text(encoding="utf-8")
        )
        for _run in range(2):
            for statement in statements:
                db.execute(statement)
                # (level, code, message). A Note is the expected "Unknown table" from
                # DROP ... IF EXISTS on the first run; anything louder is a defect.
                for level, code, message in db.query("SHOW WARNINGS"):
                    assert level == "Note", f"{code} {message} after: {statement[:60]}"

    def test_run_sql_script_reports_the_statement_count(self, db: Database) -> None:
        count = run_sql_script(db, locate_create_dw_script())
        assert count == 16 + 15

    def test_every_table_and_column_matches_the_contract(self, schema_db: Database) -> None:
        rows = schema_db.query(
            "SELECT table_name, column_name, column_type FROM information_schema.columns"
            " WHERE table_schema = DATABASE() ORDER BY table_name, ordinal_position"
        )
        actual: dict[str, list[tuple[str, str]]] = {}
        for table, column, column_type in rows:
            actual.setdefault(table, []).append((column, column_type))
        expected = {table: _columns_of(spec) for table, spec in EXPECTED_COLUMNS.items()}
        assert actual == expected

    def test_primary_keys(self, schema_db: Database) -> None:
        rows = schema_db.query(
            "SELECT table_name, column_name FROM information_schema.key_column_usage"
            " WHERE table_schema = DATABASE() AND constraint_name = 'PRIMARY'"
        )
        assert dict(rows) == EXPECTED_PRIMARY_KEYS
        assert len(rows) == len(EXPECTED_PRIMARY_KEYS), "a primary key has more than one column"

    def test_every_foreign_key_exists_and_only_the_fact_has_any(
        self, schema_db: Database
    ) -> None:
        rows = schema_db.query(
            "SELECT table_name, column_name, referenced_table_name, referenced_column_name"
            " FROM information_schema.key_column_usage"
            " WHERE table_schema = DATABASE() AND referenced_table_name IS NOT NULL"
        )
        assert set(rows) == EXPECTED_FOREIGN_KEYS

    def test_natural_keys_are_unique(self, schema_db: Database) -> None:
        rows = schema_db.query(
            "SELECT k.table_name, k.column_name FROM information_schema.key_column_usage k"
            " JOIN information_schema.table_constraints c"
            "   ON c.constraint_schema = k.constraint_schema"
            "  AND c.table_name = k.table_name AND c.constraint_name = k.constraint_name"
            " WHERE k.table_schema = DATABASE() AND c.constraint_type = 'UNIQUE'"
        )
        assert set(rows) == EXPECTED_UNIQUE_KEYS

    def test_expected_secondary_indexes(self, schema_db: Database) -> None:
        rows = schema_db.query(
            "SELECT table_name, index_name, GROUP_CONCAT(column_name ORDER BY seq_in_index)"
            " FROM information_schema.statistics WHERE table_schema = DATABASE()"
            " GROUP BY table_name, index_name"
        )
        indexes = {(table, columns) for table, _index, columns in rows}
        assert ("dim_time", "year_no,month_no") in indexes
        assert ("dim_time", "full_date") in indexes
        assert ("fact_sales", "transaction_id,product_key") in indexes
        assert ("periodic_sales_data", "release_hour,released") in indexes

    def test_every_id_column_is_case_sensitive(self, schema_db: Database) -> None:
        rows = schema_db.query(
            "SELECT table_name, column_name, collation_name FROM information_schema.columns"
            " WHERE table_schema = DATABASE() AND data_type = 'varchar'"
            " AND column_name IN ('customer_id', 'product_id', 'supplier_id', 'transaction_id')"
        )
        assert len(rows) >= 14
        assert {collation for _table, _column, collation in rows} == {"utf8mb4_bin"}

    def test_every_table_is_innodb_utf8mb4(self, schema_db: Database) -> None:
        rows = schema_db.query(
            "SELECT table_name, engine, table_collation FROM information_schema.tables"
            " WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'"
        )
        assert len(rows) == len(EXPECTED_COLUMNS)
        for table, engine, collation in rows:
            assert engine == "InnoDB", table
            assert collation.startswith("utf8mb4_"), table

    def test_nothing_left_over_after_a_rerun(self, schema_db: Database) -> None:
        keys = _seed_dimensions(schema_db)
        _insert_fact(schema_db, keys)
        run_sql_script(schema_db, locate_create_dw_script())
        assert schema_db.query("SELECT COUNT(*) FROM fact_sales") == [(0,)]
        assert schema_db.query("SELECT COUNT(*) FROM dim_customer") == [(0,)]

    def test_drop_order_works_when_the_view_exists(self, schema_db: Database) -> None:
        keys = _seed_dimensions(schema_db)
        _insert_fact(schema_db, keys)
        schema_db.execute(
            "CREATE VIEW STORE_QUARTERLY_SALES AS"
            " SELECT s.store_name, t.year_no, t.quarter_no, SUM(f.revenue) AS total_sales"
            " FROM fact_sales f"
            " JOIN dim_store s ON s.store_key = f.store_key"
            " JOIN dim_time t ON t.time_key = f.time_key"
            " GROUP BY s.store_name, t.year_no, t.quarter_no"
        )
        assert schema_db.query("SELECT COUNT(*) FROM STORE_QUARTERLY_SALES") == [(1,)]

        run_sql_script(schema_db, locate_create_dw_script())

        views = schema_db.query(
            "SELECT COUNT(*) FROM information_schema.views WHERE table_schema = DATABASE()"
        )
        assert views == [(0,)]
        assert schema_db.query("SELECT COUNT(*) FROM fact_sales") == [(0,)]


# ----------------------------------------------------------------------- constraints


class TestConstraints:
    @pytest.mark.parametrize(
        "column",
        ["customer_key", "product_key", "supplier_key", "store_key", "time_key"],
    )
    def test_fact_rejects_a_key_missing_from_its_dimension(
        self, clean_db: Database, column: str
    ) -> None:
        keys = _seed_dimensions(clean_db)
        with pytest.raises(MySqlError) as error:
            _insert_fact(clean_db, keys, **{column: 9999})
        assert error.value.errno == ER_NO_REFERENCED_ROW

    def test_dimension_row_in_use_cannot_be_deleted(self, clean_db: Database) -> None:
        keys = _seed_dimensions(clean_db)
        _insert_fact(clean_db, keys)
        with pytest.raises(MySqlError) as error:
            clean_db.execute("DELETE FROM dim_store")
        assert error.value.errno == ER_ROW_IS_REFERENCED

    def test_valid_fact_row_is_accepted_and_timestamped(self, clean_db: Database) -> None:
        keys = _seed_dimensions(clean_db)
        assert _insert_fact(clean_db, keys) == 1
        rows = clean_db.query("SELECT revenue, loaded_at FROM fact_sales")
        assert rows[0][0] == Decimal("15.300")
        assert isinstance(rows[0][1], datetime)

    @pytest.mark.parametrize(
        "bad",
        [
            pytest.param({"quantity": 0}, id="zero-quantity"),
            pytest.param({"quantity": -1}, id="negative-quantity"),
            pytest.param({"revenue": Decimal("0")}, id="zero-revenue"),
            pytest.param({"revenue": Decimal("-1.5")}, id="negative-revenue"),
            pytest.param({"sales_mode": "Web"}, id="bad-sales-mode"),
            pytest.param({"processing_mode": "Batch"}, id="bad-processing-mode"),
        ],
    )
    def test_fact_checks(self, clean_db: Database, bad: dict[str, Any]) -> None:
        keys = _seed_dimensions(clean_db)
        with pytest.raises(MySqlError) as error:
            _insert_fact(clean_db, keys, **bad)
        assert error.value.errno == ER_CHECK_VIOLATED

    @pytest.mark.parametrize(
        ("table", "assignment"),
        [
            pytest.param("dim_customer", "gender = 'Other'", id="gender"),
            pytest.param("dim_customer", "age_group = '99'", id="age"),
            pytest.param("dim_customer", "occupation = 'Pilot'", id="occupation"),
            pytest.param("dim_customer", "city_category = 'D'", id="city"),
            pytest.param("dim_customer", "stay_in_current_city_years = '5'", id="stay"),
            pytest.param("dim_customer", "marital_status = 'Unknown'", id="marital"),
            pytest.param("dim_product", "product_category = 'Cars'", id="category"),
            pytest.param("dim_store", "store_id = 6", id="store"),
            pytest.param("dim_time", "hour_of_day = 24", id="hour"),
            pytest.param("dim_time", "day_of_month = 0", id="day"),
            pytest.param("dim_time", "month_no = 13", id="month"),
            pytest.param("dim_time", "quarter_no = 5", id="quarter"),
            pytest.param("dim_time", "day_of_week = 0", id="weekday"),
            pytest.param("dim_time", "day_type = 'Holiday'", id="day-type"),
            pytest.param("dim_time", "season = 'Monsoo'", id="season"),
            pytest.param("dim_time", "half_year = 'H3'", id="half"),
        ],
    )
    def test_dimension_checks(self, clean_db: Database, table: str, assignment: str) -> None:
        _seed_dimensions(clean_db)
        with pytest.raises(MySqlError) as error:
            clean_db.execute(f"UPDATE {table} SET {assignment}")
        assert error.value.errno == ER_CHECK_VIOLATED

    @pytest.mark.parametrize(
        ("value", "accepted"),
        [("0-17", True), ("55+", True), ("0–17", False)],
    )
    def test_age_labels_use_ascii_hyphens(
        self, clean_db: Database, value: str, accepted: bool
    ) -> None:
        """The PDF prints en dashes, but the project stores ASCII labels."""
        _seed_dimensions(clean_db)
        update = "UPDATE dim_customer SET age_group = %s"
        if accepted:
            clean_db.execute(update, (value,))
        else:
            with pytest.raises(MySqlError) as error:
                clean_db.execute(update, (value,))
            assert error.value.errno in (ER_CHECK_VIOLATED, ER_DATA_TOO_LONG)

    def test_online_rows_cannot_reach_the_shed_table(self, clean_db: Database) -> None:
        with pytest.raises(MySqlError) as error:
            clean_db.execute(
                "INSERT INTO shed_table (staging_id, transaction_id, customer_id, product_id,"
                " quantity, unit_price, event_time, sales_mode)"
                " VALUES (1, 't', 'c', 'p', 1, 1.0, '2011-01-01 00:00:00', 'Online')"
            )
        assert error.value.errno == ER_CHECK_VIOLATED

    def test_a_staging_row_is_shed_only_once(self, clean_db: Database) -> None:
        insert = (
            "INSERT INTO shed_table (staging_id, transaction_id, customer_id, product_id,"
            " quantity, unit_price, event_time, sales_mode)"
            " VALUES (1, 't', 'c', 'p', 1, 1.0, '2011-01-01 00:00:00', 'Periodic')"
        )
        clean_db.execute(insert)
        with pytest.raises(MySqlError) as error:
            clean_db.execute(insert)
        assert error.value.errno == ER_DUPLICATE_ENTRY

    def test_periodic_results_cannot_enter_the_live_cache(self, clean_db: Database) -> None:
        with pytest.raises(MySqlError) as error:
            clean_db.execute(
                "INSERT INTO live_cache (transaction_id, customer_id, gender, age_group,"
                " occupation, city_category, stay_in_current_city_years, marital_status,"
                " product_id, product_name, product_category, supplier_id, supplier_name,"
                " store_id, event_time, quantity, revenue, sales_mode, processing_mode)"
                " VALUES ('t', 'c', 'Male', '0-17', 'Clerk', 'A', '0', 'Single', 'p', 'n',"
                " 'Toys', 'SUP01', 's', 2, '2011-01-01 00:00:00', 1, 1.0, 'Periodic', 'Periodic')"
            )
        assert error.value.errno == ER_CHECK_VIOLATED

    def test_oversized_values_fail_instead_of_truncating(self, clean_db: Database) -> None:
        with pytest.raises(MySqlError) as error:
            clean_db.execute(
                "INSERT INTO etl_reject (reason) VALUES (%s)", ("x" * 41,)
            )
        assert error.value.errno == ER_DATA_TOO_LONG

    @pytest.mark.parametrize(
        ("table", "column", "extra"),
        [
            pytest.param(
                "dim_product",
                "product_id",
                ("product_name", "product_category"),
                id="dimension",
            ),
            pytest.param(
                "product_master_data",
                "product_id",
                ("product_name", "product_category", "supplier_id", "supplier_name"),
                id="master-data",
            ),
        ],
    )
    def test_product_ids_differing_by_case_stay_separate(
        self, clean_db: Database, table: str, column: str, extra: tuple[str, ...]
    ) -> None:
        columns = ", ".join((column, *extra))
        values = {
            "product_name": "n",
            "product_category": "Toys",
            "supplier_id": "SUP01",
            "supplier_name": "s",
        }
        placeholders = ", ".join(["%s"] * (1 + len(extra)))
        rest = tuple(values[name] for name in extra)
        for product_id in ("85123A", "85123a"):
            clean_db.execute(
                f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                (product_id, *rest),
            )
        assert clean_db.query(f"SELECT COUNT(*) FROM {table}") == [(2,)]
        assert clean_db.query(
            f"SELECT {column} FROM {table} WHERE {column} = %s", ("85123a",)
        ) == [("85123a",)]
        with pytest.raises(MySqlError) as error:
            clean_db.execute(
                f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", ("85123a", *rest)
            )
        assert error.value.errno == ER_DUPLICATE_ENTRY

    def test_id_comparison_is_case_sensitive_in_joins(self, clean_db: Database) -> None:
        """The upper-case row must not join to the lower-case master row."""
        clean_db.execute(
            "INSERT INTO product_master_data VALUES ('85123a', 'n', 'Toys', 'SUP01', 's')"
        )
        clean_db.execute(
            "INSERT INTO dim_product (product_id, product_name, product_category)"
            " VALUES ('85123A', 'n', 'Toys')"
        )
        rows = clean_db.query(
            "SELECT COUNT(*) FROM product_master_data m JOIN dim_product d USING (product_id)"
        )
        assert rows == [(0,)]


# -------------------------------------------------------------------- basic wrapper


class TestDatabaseBasics:
    def test_query_returns_python_values(self, clean_db: Database) -> None:
        when = datetime(2011, 12, 9, 12, 50)
        clean_db.execute(
            "INSERT INTO etl_reject (transaction_id, unit_price, event_time, reason)"
            " VALUES (%s, %s, %s, %s)",
            ("581587", Decimal("4.950"), when, "probe"),
        )
        rows = clean_db.query(
            "SELECT transaction_id, unit_price, event_time FROM etl_reject"
        )
        assert rows == [("581587", Decimal("4.950"), when)]
        assert isinstance(rows[0], tuple)
        assert isinstance(rows[0][0], str), "ids in a _bin collation must come back as text"

    def test_query_returns_empty_list_for_statements_without_rows(
        self, clean_db: Database
    ) -> None:
        assert clean_db.query("SET @probe = 1") == []
        assert clean_db.query("SELECT * FROM etl_reject") == []

    def test_execute_returns_rowcount(self, clean_db: Database) -> None:
        assert _insert_reject(clean_db, "a") == 1
        assert _insert_reject(clean_db, "a") == 1
        assert clean_db.execute("DELETE FROM etl_reject WHERE reason = %s", ("a",)) == 2

    def test_autocommit_is_on_outside_transactions(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        _insert_reject(clean_db, "autocommit")
        with Database(temp_database) as other:
            assert _reject_count(other, "autocommit") == 1

    def test_executemany_batches_and_counts(self, clean_db: Database) -> None:
        rows = [("r",) for _ in range(25)]
        assert clean_db.executemany(
            "INSERT INTO etl_reject (reason) VALUES (%s)", rows, batch_size=10
        ) == 25
        assert _reject_count(clean_db, "r") == 25

    def test_executemany_accepts_a_generator(self, clean_db: Database) -> None:
        count = clean_db.executemany(
            "INSERT INTO etl_reject (reason) VALUES (%s)", (("g",) for _ in range(7)), batch_size=3
        )
        assert count == 7

    def test_executemany_with_nothing_to_do(self, clean_db: Database) -> None:
        assert clean_db.executemany("INSERT INTO etl_reject (reason) VALUES (%s)", []) == 0

    def test_executemany_counts_non_insert_statements(self, clean_db: Database) -> None:
        clean_db.executemany(
            "INSERT INTO etl_reject (reason) VALUES (%s)", [("u1",), ("u2",), ("u3",)]
        )
        count = clean_db.executemany(
            "DELETE FROM etl_reject WHERE reason = %s", [("u1",), ("u2",), ("u3",)], batch_size=2
        )
        assert count == 3

    def test_parameters_are_data_never_sql(self, clean_db: Database) -> None:
        tricky = ("50% off; 'x' \"y\" %s `z` -- \\", "'); DROP TABLE etl_reject; -- ", "café 日本")
        clean_db.executemany(
            "INSERT INTO etl_reject (transaction_id, reason) VALUES (%s, %s)",
            [("t", value[:40]) for value in tricky],
        )
        rows = clean_db.query("SELECT reason FROM etl_reject ORDER BY reject_id")
        assert [row[0] for row in rows] == [value[:40] for value in tricky]

    def test_context_manager_closes_the_session(self, temp_database: ConnectionConfig) -> None:
        with Database(temp_database) as database:
            assert database.query("SELECT 1") == [(1,)]
        with pytest.raises(MySqlError):
            database.query("SELECT 1")

    def test_close_can_be_called_twice(self, temp_database: ConnectionConfig) -> None:
        database = Database(temp_database)
        database.close()
        database.close()

    def test_connection_id_matches_the_server(self, db: Database) -> None:
        assert db.query("SELECT CONNECTION_ID()") == [(db.connection_id,)]

    def test_session_is_utf8mb4_and_strict(self, db: Database) -> None:
        rows = db.query("SELECT @@character_set_connection, @@autocommit, @@sql_mode")
        charset, autocommit, sql_mode = rows[0]
        assert charset == "utf8mb4"
        assert autocommit == 1
        assert "STRICT_TRANS_TABLES" in sql_mode

    def test_reconnect_gives_a_new_session_with_the_same_settings(self, db: Database) -> None:
        before = db.connection_id
        db.execute("SET @probe = 1")
        db.reconnect()
        assert db.connection_id != before
        probe, autocommit, database = db.query("SELECT @probe, @@autocommit, DATABASE()")[0]
        assert probe is None
        assert autocommit == 1
        assert database is not None

    def test_a_dead_session_reports_a_real_lost_connection_code(
        self, db: Database, db_cfg: ConnectionConfig
    ) -> None:
        """run_transaction depends on this. The driver's cursor() would say errno -1."""
        _kill(db_cfg, db.connection_id)
        with pytest.raises(MySqlError) as error:
            db.query("SELECT 1")
        assert error.value.errno in LOST_CONNECTION

    def test_reconnect_recovers_a_killed_session(
        self, db: Database, db_cfg: ConnectionConfig
    ) -> None:
        _kill(db_cfg, db.connection_id)
        db.reconnect()
        assert db.query("SELECT 1") == [(1,)]


class TestNoPasswordLeak:
    SECRET = "n0t-the-real-Pa55word"

    def test_failed_login_does_not_reveal_it(self, db_cfg: ConnectionConfig) -> None:
        bad = replace(db_cfg, password=self.SECRET)
        with pytest.raises(MySqlError) as error:
            Database(bad)
        assert error.value.errno == ER_ACCESS_DENIED
        for text in (str(error.value), repr(error.value), "".join(traceback.format_exception(error.value))):
            assert self.SECRET not in text

    def test_repr_does_not_reveal_it(self, db: Database, db_cfg: ConnectionConfig) -> None:
        for text in (repr(db), str(db), repr(db_cfg), str(db_cfg)):
            assert db_cfg.password == "" or db_cfg.password not in text

    def test_failed_ensure_database_does_not_reveal_it(self, db_cfg: ConnectionConfig) -> None:
        bad = replace(db_cfg, database="t_foundation_never", password=self.SECRET)
        with pytest.raises(MySqlError) as error:
            ensure_database(bad)
        assert self.SECRET not in str(error.value)


class TestNoPasswordInLogs:
    def test_recovery_logs_stay_clean_even_at_debug_level(
        self,
        clean_db: Database,
        temp_database: ConnectionConfig,
        relay: FaultyRelay,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Runs both recovery paths with every logger open, including the driver's."""
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "logged")
            if calls == 1:
                relay.arm_lost_before_server()
            elif calls == 2:
                relay.arm_lost_reply()

        with caplog.at_level(logging.DEBUG):
            with Database(replace(temp_database, host="127.0.0.1", port=relay.port)) as session:
                session.run_transaction(work, writer="meshjoin")

        assert calls == 2
        assert "Retrying transaction after MySQL error" in caplog.text
        assert "the commit landed" in caplog.text
        assert temp_database.password == "" or temp_database.password not in caplog.text


# ---------------------------------------------------------------- schema management


class TestEnsureDatabase:
    def test_creates_utf8mb4_database_and_is_idempotent(self, db_cfg: ConnectionConfig) -> None:
        name = f"t_foundation_ensure_{time.monotonic_ns() % 10**10}"
        cfg = replace(db_cfg, database=name)
        try:
            ensure_database(cfg)
            ensure_database(cfg)
            with Database(cfg) as database:
                rows = database.query(
                    "SELECT default_character_set_name FROM information_schema.schemata"
                    " WHERE schema_name = DATABASE()"
                )
                assert rows == [("utf8mb4",)]
                assert database.query("SELECT DATABASE()") == [(name,)]
        finally:
            with Database(db_cfg) as admin:
                admin.execute(f"DROP DATABASE IF EXISTS `{name}`")

    @pytest.mark.parametrize("name", ["bad`name", "bad name", "x; DROP DATABASE mysql", ""])
    def test_refuses_an_unsafe_name_before_connecting(
        self, db_cfg: ConnectionConfig, name: str
    ) -> None:
        unreachable = replace(db_cfg, database=name, host="invalid.invalid")
        with pytest.raises(ValueError, match="Database name"):
            ensure_database(unreachable)


class TestRunSqlScript:
    def test_runs_a_script_with_tricky_text(self, db: Database, tmp_path: Path) -> None:
        script = tmp_path / "tricky.sql"
        script.write_text(
            "﻿-- it's a comment; with a semicolon\n"
            "DROP TABLE IF EXISTS script_probe;\n"
            "/* multi; line\n comment */\n"
            "CREATE TABLE script_probe (v VARCHAR(40));\n"
            "INSERT INTO script_probe VALUES ('a;b'), ('it''s; -- not a comment');\n"
            "# hash comment; here\n"
            "INSERT INTO script_probe VALUES ('last, no semicolon')",
            encoding="utf-8",
        )
        try:
            assert run_sql_script(db, script) == 4
            rows = db.query("SELECT v FROM script_probe ORDER BY v")
            assert rows == [("a;b",), ("it's; -- not a comment",), ("last, no semicolon",)]
        finally:
            db.execute("DROP TABLE IF EXISTS script_probe")


# ---------------------------------------------------------------------- transactions


class TestTransaction:
    def test_commits_on_success(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        with clean_db.transaction() as tx:
            assert tx is clean_db
            _insert_reject(tx, "commit")
            with Database(temp_database) as other:
                assert _reject_count(other, "commit") == 0, "visible before COMMIT"
        with Database(temp_database) as other:
            assert _reject_count(other, "commit") == 1

    def test_rolls_back_on_exception(self, clean_db: Database) -> None:
        with pytest.raises(LookupError):
            with clean_db.transaction() as tx:
                _insert_reject(tx, "rollback")
                raise LookupError("boom")
        assert _reject_count(clean_db, "rollback") == 0
        assert clean_db.query("SELECT @@autocommit") == [(1,)]

    def test_rolls_back_on_sql_error(self, clean_db: Database) -> None:
        with pytest.raises(MySqlError):
            with clean_db.transaction() as tx:
                _insert_reject(tx, "rollback")
                tx.execute("INSERT INTO no_such_table VALUES (1)")
        assert _reject_count(clean_db, "rollback") == 0

    def test_usable_again_after_a_rollback(self, clean_db: Database) -> None:
        with pytest.raises(LookupError):
            with clean_db.transaction():
                raise LookupError("boom")
        with clean_db.transaction() as tx:
            _insert_reject(tx, "after")
        assert _reject_count(clean_db, "after") == 1

    def test_writer_row_is_created_and_then_bumped(self, clean_db: Database) -> None:
        with clean_db.transaction("meshjoin"):
            pass
        assert _txn_seq(clean_db, "meshjoin") == 1
        with clean_db.transaction("meshjoin"):
            pass
        with clean_db.transaction("periodic"):
            pass
        assert _txn_seq(clean_db, "meshjoin") == 2
        assert _txn_seq(clean_db, "periodic") == 1

    def test_bump_keeps_the_other_progress_columns(self, clean_db: Database) -> None:
        clean_db.execute(
            "INSERT INTO etl_progress (writer, txn_seq, online_rows_committed, next_group,"
            " sim_hour, prepared) VALUES ('meshjoin', 10, 20, 30, 40, 1)"
        )
        with clean_db.transaction("meshjoin"):
            pass
        assert clean_db.query(
            "SELECT txn_seq, online_rows_committed, next_group, sim_hour, prepared"
            " FROM etl_progress WHERE writer = 'meshjoin'"
        ) == [(11, 20, 30, 40, 1)]

    def test_writer_bump_is_rolled_back_with_the_work(self, clean_db: Database) -> None:
        with clean_db.transaction("meshjoin"):
            pass
        with pytest.raises(LookupError):
            with clean_db.transaction("meshjoin"):
                raise LookupError("boom")
        assert _txn_seq(clean_db, "meshjoin") == 1

    def test_bump_gives_no_deprecation_warning(self, clean_db: Database) -> None:
        """VALUES() in ON DUPLICATE KEY UPDATE warns on 8.0.20 and later."""
        for _run in range(2):
            clean_db.execute(_BUMP_TXN_SEQ, ("warning_probe",))
            assert clean_db.query("SHOW WARNINGS") == []

    def test_nesting_is_refused(self, clean_db: Database) -> None:
        with pytest.raises(MySqlError, match="already in progress"):
            with clean_db.transaction():
                with clean_db.transaction():
                    pass


# ------------------------------------------------------------- retries on lock errors


def _lock_error(errno: int) -> MySqlError:
    return mysql.connector.errors.DatabaseError(msg="injected", errno=errno)


class TestRunTransactionLockErrors:
    def test_returns_the_result_and_passes_the_database(self, clean_db: Database) -> None:
        def work(tx: Database) -> str:
            assert tx is clean_db
            _insert_reject(tx, "ok")
            return "done"

        assert clean_db.run_transaction(work, writer="meshjoin") == "done"
        assert _reject_count(clean_db, "ok") == 1
        assert _txn_seq(clean_db, "meshjoin") == 1

    def test_retries_a_real_deadlock(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        clean_db.execute(
            "INSERT INTO etl_progress (writer, sim_hour) VALUES ('row_a', 0), ('row_b', 0)"
        )
        both_hold_first_lock = threading.Barrier(2, timeout=20)
        calls = {"row_a": 0, "row_b": 0}
        errors: list[BaseException] = []

        def worker(first: str, second: str) -> None:
            def work(tx: Database) -> None:
                calls[first] += 1
                update = "UPDATE etl_progress SET sim_hour = sim_hour + 1 WHERE writer = %s"
                tx.execute(update, (first,))
                if calls[first] == 1:
                    both_hold_first_lock.wait()
                tx.execute(update, (second,))

            try:
                with Database(temp_database) as session:
                    session.run_transaction(work)
            except BaseException as exc:  # surfaced on the main thread below
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=("row_a", "row_b")),
            threading.Thread(target=worker, args=("row_b", "row_a")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)

        assert errors == []
        # The victim of the deadlock ran twice and the survivor once. Three calls proves
        # that a real 1213 happened and was retried.
        assert sorted(calls.values()) == [1, 2]
        rows = clean_db.query(
            "SELECT writer, sim_hour FROM etl_progress WHERE writer IN ('row_a', 'row_b')"
            " ORDER BY writer"
        )
        assert rows == [("row_a", 2), ("row_b", 2)], "both transactions took full effect"

    def test_retries_a_real_lock_wait_timeout_and_undoes_the_first_attempt(
        self, clean_db: Database, temp_database: ConnectionConfig
    ) -> None:
        clean_db.execute("INSERT INTO etl_progress (writer) VALUES ('held')")
        clean_db.execute("SET SESSION innodb_lock_wait_timeout = 1")
        calls = 0
        with Database(temp_database) as holder:
            holder_tx = holder.transaction()
            holder_tx.__enter__()
            holder.execute("UPDATE etl_progress SET sim_hour = 99 WHERE writer = 'held'")

            def work(tx: Database) -> None:
                nonlocal calls
                calls += 1
                if calls == 2:
                    holder_tx.__exit__(None, None, None)
                # A lock wait timeout undoes only the failing statement by default, so
                # this marker survives into the rollback unless the wrapper rolls back.
                _insert_reject(tx, "lock_wait")
                tx.execute("UPDATE etl_progress SET next_group = 7 WHERE writer = 'held'")

            clean_db.run_transaction(work)

        assert calls == 2
        assert _reject_count(clean_db, "lock_wait") == 1
        assert clean_db.query(
            "SELECT next_group, sim_hour FROM etl_progress WHERE writer = 'held'"
        ) == [(7, 99)]

    @pytest.mark.parametrize("errno", [1213, 1205])
    def test_gives_up_after_the_retry_budget(self, clean_db: Database, errno: int) -> None:
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "budget")
            raise _lock_error(errno)

        with pytest.raises(MySqlError) as error:
            clean_db.run_transaction(work, retries=2)
        assert error.value.errno == errno
        assert calls == 3, "the first attempt plus two retries"
        assert _reject_count(clean_db, "budget") == 0

    def test_zero_retries_means_one_attempt(self, clean_db: Database) -> None:
        calls = 0

        def work(_tx: Database) -> None:
            nonlocal calls
            calls += 1
            raise _lock_error(1213)

        with pytest.raises(MySqlError):
            clean_db.run_transaction(work, retries=0)
        assert calls == 1

    @pytest.mark.parametrize(
        ("sql", "errno"),
        [
            pytest.param("INSERT INTO no_such_table VALUES (1)", ER_NO_SUCH_TABLE, id="missing-table"),
            pytest.param(
                "INSERT INTO etl_reject (reason) VALUES (REPEAT('x', 41))",
                ER_DATA_TOO_LONG,
                id="data-too-long",
            ),
        ],
    )
    def test_other_sql_errors_are_not_retried(
        self, clean_db: Database, sql: str, errno: int
    ) -> None:
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            tx.execute(sql)

        with pytest.raises(MySqlError) as error:
            clean_db.run_transaction(work, writer="meshjoin")
        assert error.value.errno == errno
        assert calls == 1
        assert _txn_seq(clean_db, "meshjoin") == 0

    def test_python_errors_are_not_retried(self, clean_db: Database) -> None:
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "py")
            raise KeyError("bug")

        with pytest.raises(KeyError):
            clean_db.run_transaction(work)
        assert calls == 1
        assert _reject_count(clean_db, "py") == 0


# ------------------------------------------------------------------- lost connections


class FaultyRelay:
    """TCP relay in front of MySQL that loses the connection on the next client packet.

    It never parses the protocol, so it works over TLS. A test arms a fault at the end of
    its transaction body, which makes COMMIT the next packet the client sends.
    """

    def __init__(self, cfg: ConnectionConfig) -> None:
        self._target = (cfg.host, cfg.port)
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port: int = self._listener.getsockname()[1]
        self._fault: Callable[[socket.socket, socket.socket, bytes], None] | None = None
        self._threads: list[threading.Thread] = []
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def close(self) -> None:
        self._listener.close()
        for thread in self._threads:
            thread.join(timeout=5)

    def _accept_loop(self) -> None:
        while True:
            try:
                client, _address = self._listener.accept()
            except OSError:
                return
            server = socket.create_connection(self._target)
            thread = threading.Thread(target=self._relay, args=(client, server), daemon=True)
            self._threads.append(thread)
            thread.start()

    def _relay(self, client: socket.socket, server: socket.socket) -> None:
        try:
            while True:
                readable, _, _ = select.select([client, server], [], [])
                if server in readable:
                    data = server.recv(65536)
                    if not data:
                        return
                    client.sendall(data)
                if client in readable:
                    data = client.recv(65536)
                    if not data:
                        return
                    fault, self._fault = self._fault, None
                    if fault is not None:
                        fault(client, server, data)
                        return
                    server.sendall(data)
        except OSError:
            return
        finally:
            client.close()
            server.close()

    def arm_lost_before_server(self, late_seconds: float = 0.0) -> None:
        """COMMIT never reaches the server. The server only notices the dead client
        after late_seconds, so until then the transaction is still open."""

        def fault(client: socket.socket, _server: socket.socket, _data: bytes) -> None:
            client.close()
            time.sleep(late_seconds)

        self._fault = fault

    def arm_lost_reply(self) -> None:
        """COMMIT is applied by the server and only the reply is lost."""

        def fault(_client: socket.socket, server: socket.socket, data: bytes) -> None:
            server.sendall(data)
            server.recv(65536)

        self._fault = fault

    def arm_late_commit(self, late_seconds: float) -> None:
        """The client gives up first, and COMMIT lands after the client reconnected."""

        def fault(client: socket.socket, server: socket.socket, data: bytes) -> None:
            client.close()
            time.sleep(late_seconds)
            server.sendall(data)
            server.recv(65536)

        self._fault = fault


@pytest.fixture
def relay(db_cfg: ConnectionConfig) -> Iterator[FaultyRelay]:
    faulty = FaultyRelay(db_cfg)
    yield faulty
    faulty.close()


@pytest.fixture
def relayed_db(
    clean_db: Database, temp_database: ConnectionConfig, relay: FaultyRelay
) -> Iterator[Database]:
    """A session whose traffic goes through the relay. The relay connects to the real
    host itself, so the session only needs the relay's loopback address and port."""
    with Database(replace(temp_database, host="127.0.0.1", port=relay.port)) as session:
        yield session


class TestRunTransactionLostConnection:
    WRITER = "meshjoin"

    def test_kill_mid_transaction_runs_fn_again(
        self, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        calls = 0
        first_session = clean_db.connection_id

        def work(tx: Database) -> str:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "mid")
            if calls == 1:
                _kill(db_cfg, tx.connection_id)
                _insert_reject(tx, "mid")  # fails: the session is gone
            return f"attempt {calls}"

        assert clean_db.run_transaction(work, writer=self.WRITER) == "attempt 2"
        assert calls == 2
        assert clean_db.connection_id != first_session
        assert _reject_count(clean_db, "mid") == 1
        assert _txn_seq(clean_db, self.WRITER) == 1, "the killed attempt must not count"

    def test_kill_just_before_commit_rolls_back_and_runs_fn_again(
        self, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        with clean_db.transaction(self.WRITER):
            pass  # the sequence starts from an existing row
        calls = 0

        def work(tx: Database) -> str:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "at_commit")
            if calls == 1:
                _kill(db_cfg, tx.connection_id)
            return f"attempt {calls}"

        assert clean_db.run_transaction(work, writer=self.WRITER) == "attempt 2"
        assert calls == 2
        assert _reject_count(clean_db, "at_commit") == 1
        assert _txn_seq(clean_db, self.WRITER) == 2

    def test_kill_before_the_writer_row_ever_existed(
        self, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        """Resolving must cope with no row at all, not just with an old value."""
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "first_ever")
            if calls == 1:
                _kill(db_cfg, tx.connection_id)

        clean_db.run_transaction(work, writer="brand_new")
        assert calls == 2
        assert _reject_count(clean_db, "first_ever") == 1
        assert _txn_seq(clean_db, "brand_new") == 1

    def test_kill_while_a_statement_is_running(
        self, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "running")
            if calls == 1:
                timer = threading.Timer(0.3, _kill, args=(db_cfg, tx.connection_id))
                timer.start()
                try:
                    tx.query("SELECT SLEEP(10)")
                finally:
                    timer.join()

        clean_db.run_transaction(work, writer=self.WRITER)
        assert calls == 2
        assert _reject_count(clean_db, "running") == 1

    def test_session_dead_before_the_transaction_starts(
        self, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        """An idle connection the server dropped must not fail the next transaction."""
        _kill(db_cfg, clean_db.connection_id)
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "was_idle")

        clean_db.run_transaction(work, writer=self.WRITER)
        assert calls == 1
        assert _reject_count(clean_db, "was_idle") == 1

    def test_commit_applied_but_reply_lost_does_not_run_fn_again(
        self, clean_db: Database, relayed_db: Database, relay: FaultyRelay
    ) -> None:
        calls = 0

        def work(tx: Database) -> str:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "reply_lost")
            relay.arm_lost_reply()
            return f"attempt {calls}"

        first_session = relayed_db.connection_id
        assert relayed_db.run_transaction(work, writer=self.WRITER) == "attempt 1"
        assert calls == 1, "the commit landed, so running fn again would load twice"
        assert relayed_db.connection_id != first_session, "the connection really was lost"
        assert _reject_count(clean_db, "reply_lost") == 1
        assert _txn_seq(clean_db, self.WRITER) == 1

    def test_commit_never_reaching_the_server_runs_fn_again(
        self, clean_db: Database, relayed_db: Database, relay: FaultyRelay
    ) -> None:
        calls = 0

        def work(tx: Database) -> str:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "never_arrived")
            if calls == 1:
                relay.arm_lost_before_server()
            return f"attempt {calls}"

        first_session = relayed_db.connection_id
        assert relayed_db.run_transaction(work, writer=self.WRITER) == "attempt 2"
        assert calls == 2
        assert relayed_db.connection_id != first_session
        assert _reject_count(clean_db, "never_arrived") == 1
        assert _txn_seq(clean_db, self.WRITER) == 1

    def test_commit_landing_late_is_waited_for(
        self, clean_db: Database, relayed_db: Database, relay: FaultyRelay
    ) -> None:
        """The race a plain read gets wrong: the old transaction is still open when the
        client reconnects. A plain read sees the old sequence and runs fn again, and then
        the late COMMIT lands as well, so the work is loaded twice."""
        calls = 0
        late_seconds = 1.0

        def work(tx: Database) -> str:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "late")
            relay.arm_late_commit(late_seconds)
            return f"attempt {calls}"

        started = time.monotonic()
        assert relayed_db.run_transaction(work, writer=self.WRITER) == "attempt 1"
        assert time.monotonic() - started >= late_seconds * 0.8, "must wait for the old txn"
        assert calls == 1
        assert _reject_count(clean_db, "late") == 1
        assert _txn_seq(clean_db, self.WRITER) == 1

    def test_abandoned_transaction_is_waited_for_then_fn_runs_again(
        self, clean_db: Database, relayed_db: Database, relay: FaultyRelay
    ) -> None:
        """The same wait, when the server is slow to notice and then rolls back."""
        calls = 0
        late_seconds = 1.0

        def work(tx: Database) -> str:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "abandoned")
            if calls == 1:
                relay.arm_lost_before_server(late_seconds)
            return f"attempt {calls}"

        assert relayed_db.run_transaction(work, writer=self.WRITER) == "attempt 2"
        assert calls == 2
        assert _reject_count(clean_db, "abandoned") == 1
        assert _txn_seq(clean_db, self.WRITER) == 1

    def test_outcome_stuck_behind_a_lock_is_reported_not_guessed(
        self, clean_db: Database, relayed_db: Database, relay: FaultyRelay
    ) -> None:
        """If the old transaction outlives the lock wait, nothing is known yet. Running
        fn again could load twice, so the error must say so instead."""
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "stuck")
            relay.arm_late_commit(3.0)

        real_reconnect = relayed_db.reconnect

        def reconnect_with_short_wait() -> None:
            real_reconnect()
            relayed_db.execute("SET SESSION innodb_lock_wait_timeout = 1")

        relayed_db.reconnect = reconnect_with_short_wait  # type: ignore[method-assign]
        with pytest.raises(CommitOutcomeUnknownError):
            relayed_db.run_transaction(work, writer=self.WRITER)
        assert calls == 1
        time.sleep(3.0)
        assert _reject_count(clean_db, "stuck") == 1, "exactly the one late commit"

    def test_commit_lost_without_a_writer_is_reported(
        self, clean_db: Database, relayed_db: Database, relay: FaultyRelay
    ) -> None:
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "no_writer")
            relay.arm_lost_reply()

        with pytest.raises(CommitOutcomeUnknownError):
            relayed_db.run_transaction(work)
        assert calls == 1
        assert relayed_db.query("SELECT 1") == [(1,)], "the session is usable afterwards"

    def test_lost_before_commit_without_a_writer_runs_fn_again(
        self, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "no_writer_mid")
            if calls == 1:
                _kill(db_cfg, tx.connection_id)
                _insert_reject(tx, "no_writer_mid")

        clean_db.run_transaction(work)
        assert calls == 2
        assert _reject_count(clean_db, "no_writer_mid") == 1

    def test_gives_up_when_every_attempt_loses_the_connection(
        self, clean_db: Database, db_cfg: ConnectionConfig
    ) -> None:
        calls = 0

        def work(tx: Database) -> None:
            nonlocal calls
            calls += 1
            _insert_reject(tx, "doomed")
            _kill(db_cfg, tx.connection_id)
            _insert_reject(tx, "doomed")

        with pytest.raises(MySqlError) as error:
            clean_db.run_transaction(work, writer=self.WRITER, retries=2)
        assert error.value.errno in LOST_CONNECTION
        assert calls == 3
        assert _reject_count(clean_db, "doomed") == 0
        assert clean_db.query("SELECT 1") == [(1,)], "left with a working session"

    def test_a_series_of_transactions_stays_exact_across_faults(
        self, clean_db: Database, relayed_db: Database, relay: FaultyRelay
    ) -> None:
        """Mixed outcomes over one session: the count of rows must equal the count of
        logical transactions, and txn_seq must count only real commits."""
        faults: list[Callable[[], None] | None] = [
            None,
            relay.arm_lost_reply,
            None,
            relay.arm_lost_before_server,
            relay.arm_lost_reply,
            None,
        ]
        for fault in faults:
            armed = False

            def work(tx: Database) -> None:
                nonlocal armed
                _insert_reject(tx, "series")
                if fault is not None and not armed:
                    armed = True
                    fault()

            relayed_db.run_transaction(work, writer=self.WRITER)

        assert _reject_count(clean_db, "series") == len(faults)
        assert _txn_seq(clean_db, self.WRITER) == len(faults)
