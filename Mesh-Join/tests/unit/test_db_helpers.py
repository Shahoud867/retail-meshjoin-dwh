"""The parts of retail_dw.db that need no server."""

from __future__ import annotations

from pathlib import Path

import pytest

from retail_dw import db as db_module
from retail_dw.db import locate_create_dw_script, validate_database_name


@pytest.mark.parametrize(
    "name", ["a", "retail_dw", "RetailDW2026", "0", "x" * 64, "t_foundation_1"]
)
def test_valid_database_names_are_returned(name: str) -> None:
    assert validate_database_name(name) == name


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("", id="empty"),
        pytest.param("x" * 65, id="too-long"),
        pytest.param("retail dw", id="space"),
        pytest.param("retail-dw", id="hyphen"),
        pytest.param("retail.dw", id="dot"),
        pytest.param("retail`dw", id="backtick"),
        pytest.param("a`; DROP DATABASE mysql; -- ", id="injection"),
        pytest.param("retail_dw\n", id="trailing-newline"),
        pytest.param("\nretail_dw", id="leading-newline"),
        pytest.param("réetail", id="non-ascii-letter"),
        pytest.param("٣", id="non-ascii-digit"),
    ],
)
def test_unsafe_database_names_are_refused(name: str) -> None:
    with pytest.raises(ValueError, match="Database name"):
        validate_database_name(name)


def test_locates_the_schema_script_from_any_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = locate_create_dw_script()
    assert path.is_absolute()
    assert path.parts[-2:] == ("Create-DW", "create_dw.sql")
    assert path.is_file()


def test_missing_schema_script_gives_a_clear_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_module = tmp_path / "Mesh-Join" / "retail_dw" / "db.py"
    monkeypatch.setattr(db_module, "__file__", str(fake_module))
    with pytest.raises(FileNotFoundError, match="create_dw.sql"):
        locate_create_dw_script()
