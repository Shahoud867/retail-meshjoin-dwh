"""The graded queries, read from Queries-DW/queries_dw.sql by their '-- @query' labels, so the
dashboard always shows exactly the submitted SQL. Only these ids can ever be run."""

from __future__ import annotations

import re
from pathlib import Path

from retail_dw.db import split_sql_statements

SCRIPT = Path(__file__).resolve().parents[3] / "Queries-DW" / "queries_dw.sql"
_LABEL = re.compile(r"^--\s*@query\s+(\w+)\s*$", re.M)
_BANNER = re.compile(r"^--\s*(Q\d+)\.\s*(.+)$", re.M)
# DDL, which a read-only dashboard must never run.
_NOT_EXPOSED = {"Q20_view"}


def load_queries(path: Path = SCRIPT) -> dict[str, str]:
    """Map each query id to its one SQL statement, in script order."""
    text = path.read_text(encoding="utf-8")
    labels = list(_LABEL.finditer(text))
    queries: dict[str, str] = {}
    for position, label in enumerate(labels):
        end = labels[position + 1].start() if position + 1 < len(labels) else len(text)
        statements = split_sql_statements(text[label.end() : end])
        if statements and label.group(1) not in _NOT_EXPOSED:
            queries[label.group(1)] = statements[0]
    return queries


def load_titles(path: Path = SCRIPT) -> dict[str, str]:
    """The question titles from the script's banner comments."""
    text = path.read_text(encoding="utf-8")
    return {match.group(1): match.group(2).strip() for match in _BANNER.finditer(text)}
