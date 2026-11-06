"""Shared fixtures. The test server comes from DWH_TEST_* environment variables only,
so no connection detail is ever written into a test."""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Iterator
from dataclasses import replace

import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.db import (
    Database,
    ensure_database,
    locate_create_dw_script,
    run_sql_script,
    validate_database_name,
)

_ENV_VARS = ("DWH_TEST_HOST", "DWH_TEST_PORT", "DWH_TEST_USER", "DWH_TEST_PASSWORD")


def _missing_env_reason() -> str | None:
    """Explain what is missing. An empty password is allowed, so test for presence."""
    missing = [name for name in _ENV_VARS if name not in os.environ]
    if not missing:
        return None
    return f"integration test needs a MySQL server; not set: {', '.join(missing)}"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip tests marked integration when there is no server configured, including
    tests that do not happen to use one of the fixtures below."""
    reason = _missing_env_reason()
    if reason is None:
        return
    for item in items:
        if item.get_closest_marker("integration"):
            item.add_marker(pytest.mark.skip(reason=reason))


@pytest.fixture(scope="session")
def db_cfg() -> ConnectionConfig:
    """Connection to the test server, with no database selected."""
    reason = _missing_env_reason()
    if reason is not None:
        pytest.skip(reason)
    return ConnectionConfig(
        host=os.environ["DWH_TEST_HOST"],
        port=int(os.environ["DWH_TEST_PORT"]),
        database="",
        user=os.environ["DWH_TEST_USER"],
        password=os.environ["DWH_TEST_PASSWORD"],
    )


@pytest.fixture(scope="module")
def temp_database(
    db_cfg: ConnectionConfig, request: pytest.FixtureRequest
) -> Iterator[ConnectionConfig]:
    """Create an empty, uniquely named database for one test module and drop it after.
    The module name is part of the database name, so a leftover shows where it came
    from, and the random part keeps parallel runs apart."""
    stem = re.sub(r"[^a-z0-9_]", "_", request.path.stem.lower())[:40]
    name = validate_database_name(f"t_{stem}_{uuid.uuid4().hex[:8]}")
    cfg = replace(db_cfg, database=name)
    ensure_database(cfg)
    try:
        yield cfg
    finally:
        with Database(db_cfg) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS `{name}`")


@pytest.fixture
def db(temp_database: ConnectionConfig) -> Iterator[Database]:
    """An open session on the module's database."""
    with Database(temp_database) as database:
        yield database


@pytest.fixture
def schema_db(db: Database) -> Database:
    """An open session on a freshly created schema. Runs the real script, which is
    about a second of DDL, and AUTO_INCREMENT counters start again from 1. Prefer
    clean_db unless a test needs that or changes the schema itself."""
    run_sql_script(db, locate_create_dw_script())
    return db


@pytest.fixture(scope="module")
def _module_schema(temp_database: ConnectionConfig) -> None:
    with Database(temp_database) as session:
        run_sql_script(session, locate_create_dw_script())


@pytest.fixture
def clean_db(_module_schema: None, db: Database) -> Database:
    """An open session on the schema with every table empty. The schema is created once
    per module and later tests only delete rows, which costs little more than opening
    the session. AUTO_INCREMENT counters therefore keep counting: read generated keys
    back instead of assuming they start from 1."""
    tables = db.query(
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'"
        " ORDER BY table_name = 'fact_sales' DESC"
    )
    for (table,) in tables:
        db.execute(f"DELETE FROM `{table}`")
    return db
