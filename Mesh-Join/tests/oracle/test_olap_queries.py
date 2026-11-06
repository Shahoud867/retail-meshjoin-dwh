"""Run every query of Queries-DW/queries_dw.sql and compare it with the pandas oracle.

Rows are compared one by one with exact decimals and no tolerance. By default the database
is the star loaded straight from the raw file. Set DWH_OLAP_DB to run the same checks
against a warehouse loaded by the pipeline. Each run records its timings as evidence.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import mysql.connector
import pandas as pd
import pytest
from mysql.connector import Error as MySqlError
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract

from retail_dw.config import ConnectionConfig
from tests.oracle.oracle import QUESTION_IDS, Answer, expected_answer, money, product_days
from tests.oracle.query_script import QUERY_SCRIPT, labelled_statements

pytestmark = pytest.mark.integration

TIMINGS_FILE = (
    Path(__file__).resolve().parents[3] / "Project-Report" / "evidence" / "olap_timings.json"
)
STATEMENT_LABELS = [*[f"Q{number}" for number in range(1, 20)], "Q20_view", "Q20"]
MAX_QUERY_SECONDS = 120
_FIRST_DIFFERENCES_SHOWN = 5

# product_name uses a case-insensitive collation in SQL and pandas sorts by code point, so
# the row order of Q13 cannot be matched directly. Its rows are compared after sorting both
# sides, and test_q13_groups_by_store_then_supplier_then_name checks the order itself.
_COMPARED_AS_SORTED = frozenset({"Q13"})


@dataclass(frozen=True)
class Executed:
    """One statement's result, or the error that stopped it."""

    columns: tuple[str, ...]
    rows: list[tuple]
    seconds: float
    error: str | None = None


def _connect(cfg: ConnectionConfig) -> MySQLConnectionAbstract:
    """A session in the exact mode the assignment needs, set here and not inherited, so
    these checks cannot pass only because a server happens to be configured loosely."""
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        autocommit=True,
        use_pure=True,
        sql_mode="ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO",
    )


def _run_statement(cursor: MySQLCursorAbstract, statement: str) -> Executed:
    started = time.perf_counter()
    try:
        cursor.execute(statement)
        rows = cursor.fetchall() if cursor.with_rows else []
        columns = tuple(cursor.column_names) if cursor.with_rows else ()
    except MySqlError as exc:
        return Executed((), [], time.perf_counter() - started, f"MySQL error {exc.errno}: {exc.msg}")
    return Executed(columns, rows, time.perf_counter() - started)


def _write_timings(database: str, executed: dict[str, Executed]) -> None:
    TIMINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "database": database,
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "limit_seconds": MAX_QUERY_SECONDS,
        "slowest_seconds": round(max(result.seconds for result in executed.values()), 3),
        "queries": [
            {
                "query": label,
                "seconds": round(result.seconds, 3),
                "rows": len(result.rows),
                "error": result.error,
            }
            for label, result in executed.items()
        ],
    }
    TIMINGS_FILE.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


@pytest.fixture(scope="module")
def session(olap_cfg: ConnectionConfig) -> Iterator[MySQLCursorAbstract]:
    """A cursor on the warehouse, with a server-side limit so a runaway query fails as a
    failed test and does not hang the run."""
    connection = _connect(olap_cfg)
    try:
        cursor = connection.cursor(buffered=True)
        cursor.execute(f"SET SESSION max_execution_time = {MAX_QUERY_SECONDS * 1000}")
        yield cursor
        cursor.close()
    finally:
        connection.close()


@pytest.fixture(scope="module")
def executed(session: MySQLCursorAbstract, olap_cfg: ConnectionConfig) -> dict[str, Executed]:
    """Run the whole script once, in file order, and record the timings. One bad query is
    reported by its own test, and does not hide the results of the others."""
    results = {
        label: _run_statement(session, statement)
        for label, statement in labelled_statements().items()
    }
    _write_timings(olap_cfg.database, results)
    return results


def _result(executed: dict[str, Executed], label: str) -> Executed:
    result = executed[label]
    assert result.error is None, f"{label} failed: {result.error}"
    return result


def _comparable(value: object) -> object:
    """Exact types only. A float here would mean a query lost exactness somewhere, so it is
    rejected and not rounded to fit. bytes would mean a collation or charset is wrong."""
    assert not isinstance(value, float | bytes | bytearray), f"inexact or raw value: {value!r}"
    return value


def _assert_rows_equal(label: str, actual: list[tuple], expected: list[tuple]) -> None:
    """Compare row by row, and on failure show the first rows that differ."""
    differences = []
    for index, (got, want) in enumerate(zip(actual, expected, strict=False)):
        if tuple(map(_comparable, got)) != tuple(want):
            differences.append(f"row {index}: sql {got} != oracle {want}")
            if len(differences) == _FIRST_DIFFERENCES_SHOWN:
                break
    assert len(actual) == len(expected), (
        f"{label}: sql has {len(actual):,} rows, oracle has {len(expected):,}. "
        + "; ".join(differences)
    )
    assert not differences, f"{label}:\n" + "\n".join(differences)


def _sort_key(row: tuple) -> tuple:
    """Puts NULL first without ever comparing None with another type."""
    return tuple((value is None, value) for value in row)


@pytest.fixture(scope="module")
def answers() -> dict[str, Answer]:
    """Answers computed so far. Q16 and Q17 take seconds, so each is computed only once."""
    return {}


def _answer(answers: dict[str, Answer], question: str, frame: pd.DataFrame) -> Answer:
    if question not in answers:
        answers[question] = expected_answer(question, frame)
    return answers[question]


class TestAgainstOracle:
    @pytest.mark.parametrize("question", QUESTION_IDS)
    def test_rows_match_exactly(
        self,
        question: str,
        executed: dict[str, Executed],
        raw_frame: pd.DataFrame,
        answers: dict[str, Answer],
    ) -> None:
        result = _result(executed, question)
        answer = _answer(answers, question, raw_frame)
        assert result.columns == answer.columns
        if question in _COMPARED_AS_SORTED:
            actual = sorted(result.rows, key=_sort_key)
            expected = sorted(answer.rows, key=_sort_key)
        else:
            actual, expected = result.rows, answer.rows
        _assert_rows_equal(question, actual, expected)

    @pytest.mark.parametrize("label", STATEMENT_LABELS)
    def test_query_is_within_the_time_limit(
        self, label: str, executed: dict[str, Executed]
    ) -> None:
        result = _result(executed, label)
        assert result.seconds < MAX_QUERY_SECONDS


class TestKnownAnswers:
    """Figures measured separately from the oracle and written down in the plan."""

    def test_q16_top_pairs(self, executed: dict[str, Executed]) -> None:
        rows = _result(executed, "Q16").rows
        assert [(row[0], row[2], row[4]) for row in rows] == [
            ("22386", "85099B", 546),
            ("22697", "22699", 541),
            ("22726", "22727", 530),
            ("20725", "22384", 523),
            ("20725", "22383", 519),
        ]
        assert all(row[1] and row[3] for row in rows), "pairs must show product names"

    def test_q19_row_and_product_counts(self, executed: dict[str, Executed]) -> None:
        rows = _result(executed, "Q19").rows
        assert len(rows) == 22_885
        assert len({row[0] for row in rows}) == 2_975

    def test_q19_is_measured_over_222370_product_days(
        self, session: MySQLCursorAbstract, raw_frame: pd.DataFrame
    ) -> None:
        assert product_days(raw_frame) == 222_370
        session.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT f.product_key, t.full_date"
            " FROM fact_sales AS f JOIN dim_time AS t ON t.time_key = f.time_key) AS product_days"
        )
        assert session.fetchone()[0] == 222_370

    def test_q17_totals(self, executed: dict[str, Executed]) -> None:
        rows = _result(executed, "Q17").rows
        assert rows[-1] == ("Grand total", None, None, None, None, None, Decimal("8911407.904"))
        year_totals = {row[1]: row[6] for row in rows if row[0] == "Year total"}
        assert year_totals == {2010: Decimal("572713.890"), 2011: Decimal("8338694.014")}

    def test_q17_labels_every_level(self, executed: dict[str, Executed]) -> None:
        levels = {row[0] for row in _result(executed, "Q17").rows}
        assert levels == {
            "Product", "Supplier subtotal", "Store subtotal", "Year total", "Grand total",
        }

    def test_q20_total_equals_the_fact_total(
        self, session: MySQLCursorAbstract, executed: dict[str, Executed]
    ) -> None:
        total = sum(row[3] for row in _result(executed, "Q20").rows)
        session.execute("SELECT SUM(revenue) FROM fact_sales")
        fact_total = session.fetchone()[0]
        assert total == fact_total == Decimal("8911407.904")

    @pytest.mark.parametrize(("label", "column"), [("Q4", 6), ("Q18", 7)])
    def test_2011_revenue(self, label: str, column: int, executed: dict[str, Executed]) -> None:
        total = sum(row[column] for row in _result(executed, label).rows)
        assert total == money(8_338_694_014)


class TestQuestionSpecificRules:
    def test_q1_ranks_once_over_the_year(self, executed: dict[str, Executed]) -> None:
        rows = _result(executed, "Q1").rows
        products = {(row[0], row[2]) for row in rows}
        assert sorted(day_type for day_type, _ in products) == ["Weekday"] * 5 + ["Weekend"] * 5
        assert {row[4] for row in rows} <= set(range(1, 13))

    def test_q11_ranks_again_inside_each_month(self, executed: dict[str, Executed]) -> None:
        rows = _result(executed, "Q11").rows
        assert len(rows) == 12 * 2 * 5
        assert len({(row[2], row[4]) for row in rows}) > 10, "the lists must change by month"

    def test_q6_uses_july_to_december_2011(self, executed: dict[str, Executed]) -> None:
        months = {(row[2], row[3]) for row in _result(executed, "Q6").rows}
        assert months == {(2011, month) for month in range(7, 13)}

    def test_q9_january_uses_december_2010_as_base(self, executed: dict[str, Executed]) -> None:
        rows = _result(executed, "Q9").rows
        assert {row[2] for row in rows} == set(range(1, 13))
        assert all(row[6] is not None for row in rows), "growth must exist for every month"

    def test_q12_first_quarter_uses_q4_2010_as_base(self, executed: dict[str, Executed]) -> None:
        rows = _result(executed, "Q12").rows
        assert len(rows) == 5 * 4
        assert all(row[5] is not None for row in rows), "growth must exist for every quarter"

    def test_q15_covers_all_months_of_every_pair(self, executed: dict[str, Executed]) -> None:
        rows = _result(executed, "Q15").rows
        assert len(rows) == 5 * 5 * 13
        assert sum(row[7] is None for row in rows) == 5 * 5, "only the first month has no base"

    def test_q13_groups_by_store_then_supplier_then_name(
        self, executed: dict[str, Executed]
    ) -> None:
        rows = _result(executed, "Q13").rows
        store_and_supplier = [(row[0], row[1]) for row in rows]
        assert store_and_supplier == sorted(store_and_supplier)
        for store in {row[0] for row in rows}:
            shares = {(row[1], row[5]) for row in rows if row[0] == store}
            assert abs(sum(share for _, share in shares) - 100) < Decimal("0.001")

    def test_q20_view_is_a_view_with_no_order_by(
        self, session: MySQLCursorAbstract, executed: dict[str, Executed]
    ) -> None:
        _result(executed, "Q20_view")
        session.execute(
            "SELECT table_type FROM information_schema.tables"
            " WHERE table_schema = DATABASE() AND table_name = 'STORE_QUARTERLY_SALES'"
        )
        assert session.fetchone()[0] == "VIEW"
        session.execute(
            "SELECT view_definition FROM information_schema.views"
            " WHERE table_schema = DATABASE() AND table_name = 'STORE_QUARTERLY_SALES'"
        )
        assert "order by" not in session.fetchone()[0].lower()

    def test_q20_reader_orders_by_store_year_quarter(self, executed: dict[str, Executed]) -> None:
        keys = [row[:3] for row in _result(executed, "Q20").rows]
        assert keys == sorted(keys)
        assert len(keys) == 5 * 5

    def test_session_enforces_only_full_group_by(self, session: MySQLCursorAbstract) -> None:
        session.execute("SELECT @@SESSION.sql_mode")
        assert "ONLY_FULL_GROUP_BY" in session.fetchone()[0]

    def test_loaded_dates_match_the_script_header(self, session: MySQLCursorAbstract) -> None:
        session.execute(
            "SELECT DATE_FORMAT(MIN(t.full_datetime), '%Y-%m-%d %H:%i'),"
            " DATE_FORMAT(MAX(t.full_datetime), '%Y-%m-%d %H:%i')"
            " FROM fact_sales AS f JOIN dim_time AS t ON t.time_key = f.time_key"
        )
        first, last = session.fetchone()
        header = QUERY_SCRIPT.read_text(encoding="utf-8").split("Q1. Top")[0]
        assert first in header
        assert last in header


class TestScriptText:
    """Requirements on the script itself, not on its results. These need no database, but
    they share the marker of this module because they belong with the results above."""

    def test_labels_are_complete_and_in_question_order(self) -> None:
        assert list(labelled_statements()) == STATEMENT_LABELS

    def test_script_selects_no_database_and_stores_nothing(self) -> None:
        statements = labelled_statements()
        for label, statement in statements.items():
            first_word = statement.split(maxsplit=1)[0].upper()
            allowed = {"CREATE"} if label == "Q20_view" else {"SELECT", "WITH"}
            assert first_word in allowed, f"{label} starts with {first_word}"

    def test_label_comes_directly_before_its_statement(self) -> None:
        lines = QUERY_SCRIPT.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            if line.startswith("-- @query "):
                following = lines[index + 1]
                assert following.split(maxsplit=1)[0].upper() in {"SELECT", "WITH", "CREATE"}

    @pytest.mark.parametrize("label", STATEMENT_LABELS)
    def test_no_surrogate_key_is_hardcoded(self, label: str) -> None:
        statement = labelled_statements()[label]
        assert not re.search(r"_key\s*(=|<|>|<=|>=|<>|!=|IN\s*\()\s*\d", statement, re.IGNORECASE)

    def test_q6_has_no_hardcoded_months(self) -> None:
        statement = labelled_statements()["Q6"]
        assert "DENSE_RANK" in statement
        assert "2011" not in statement

    @pytest.mark.parametrize("label", ["Q9", "Q12", "Q15"])
    def test_growth_uses_lag_and_protects_a_zero_base(self, label: str) -> None:
        statement = labelled_statements()[label]
        assert "LAG(" in statement
        assert "NULLIF(" in statement

    def test_q16_counts_distinct_carts_with_an_ordered_self_join(self) -> None:
        statement = labelled_statements()["Q16"]
        # Each cart counts once even if a product appears in it twice, each pair is formed
        # once, and the pruning floor keeps the result exact (see the comment in the script).
        assert "COUNT(DISTINCT a.transaction_id) AS carts_together" in statement
        assert "b.product_key > a.product_key" in statement
        assert "WHEN COUNT(*) = 5 THEN MIN(carts_together) ELSE 1" in statement

    def test_q17_uses_rollup_and_grouping(self) -> None:
        statement = labelled_statements()["Q17"]
        assert "WITH ROLLUP" in statement
        assert "GROUPING(" in statement

    def test_q19_comment_and_query_use_days_the_product_sold(self) -> None:
        statement = labelled_statements()["Q19"]
        assert "SUM(d.daily_revenue) OVER (PARTITION BY d.product_key)" in statement

    def test_required_comments_are_present(self) -> None:
        text = QUERY_SCRIPT.read_text(encoding="utf-8")
        for phrase in (
            "2010-12-01 08:26",
            "2011-12-09 12:50",
            "no sales on any Saturday",
            "MySQL has no",
            "ends on 9 December",
            "Slice",
            "Dice",
            "Drill-down",
        ):
            assert phrase in text, f"missing comment: {phrase}"
