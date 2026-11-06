"""Find each labelled statement in Queries-DW/queries_dw.sql."""

from __future__ import annotations

import re
from pathlib import Path

from retail_dw.db import split_sql_statements

QUERY_SCRIPT = Path(__file__).resolve().parents[3] / "Queries-DW" / "queries_dw.sql"

_LABEL = re.compile(r"^-- @query (\S+)[ \t]*$", re.MULTILINE)


def labelled_statements(path: Path = QUERY_SCRIPT) -> dict[str, str]:
    """Map each label to its statement, in file order.

    Splits with the project's own splitter, so what is tested is what the script runner
    and the mysql client would execute. Raises if a label covers anything other than one
    statement, since the text would otherwise be attributed to the wrong question.
    """
    text = path.read_text(encoding="utf-8-sig")
    labels = list(_LABEL.finditer(text))
    statements: dict[str, str] = {}
    for position, label in enumerate(labels):
        end = labels[position + 1].start() if position + 1 < len(labels) else len(text)
        found = split_sql_statements(text[label.end() : end])
        if len(found) != 1:
            raise ValueError(f"{label[1]} labels {len(found)} statements, expected exactly 1")
        if label[1] in statements:
            raise ValueError(f"{label[1]} is labelled twice")
        statements[label[1]] = found[0]
    if split_sql_statements(text[: labels[0].start()] if labels else text):
        raise ValueError("A statement appears before the first @query label")
    return statements
