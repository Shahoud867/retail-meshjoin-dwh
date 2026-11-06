"""Fixtures for the OLAP checks."""

from __future__ import annotations

import os
from dataclasses import replace

import pandas as pd
import pytest

from retail_dw.config import ConnectionConfig
from retail_dw.db import validate_database_name
from tests.oracle.independent_rules import RAW_FILE, enriched_rows
from tests.oracle.star_fixture import DEFAULT_DATABASE, ensure_star


def _require_raw_file() -> None:
    if not RAW_FILE.is_file():
        pytest.skip(f"needs the raw dataset, which is not in the zip: {RAW_FILE}")


@pytest.fixture(scope="session")
def raw_frame() -> pd.DataFrame:
    """The independent reading of the raw file. Read-only, shared by the whole session."""
    _require_raw_file()
    return enriched_rows()


@pytest.fixture(scope="session")
def olap_cfg(db_cfg: ConnectionConfig) -> ConnectionConfig:
    """The warehouse under test. DWH_OLAP_DB points the same checks at a warehouse that the
    pipeline loaded. Otherwise the star loaded straight from the raw file is used, and it
    is only ever created under its own reserved name."""
    override = os.environ.get("DWH_OLAP_DB")
    if override:
        return replace(db_cfg, database=validate_database_name(override))
    _require_raw_file()
    cfg = replace(db_cfg, database=DEFAULT_DATABASE)
    ensure_star(cfg)
    return cfg
