"""The statement splitter has to agree with MySQL about where a statement ends, and
the real schema script has to keep the shape the rest of the project relies on."""

from __future__ import annotations

import re

import pytest

from retail_dw.db import SqlScriptError, locate_create_dw_script, split_sql_statements


@pytest.mark.parametrize(
    ("script", "expected"),
    [
        pytest.param("", [], id="empty"),
        pytest.param("  \n\t ", [], id="whitespace"),
        pytest.param(";;  ;", [], id="only-semicolons"),
        pytest.param("SELECT 1", ["SELECT 1"], id="no-final-semicolon"),
        pytest.param("SELECT 1;SELECT 2;", ["SELECT 1", "SELECT 2"], id="two-statements"),
        pytest.param("  SELECT 1 ;\n\n", ["SELECT 1"], id="trims-whitespace"),
        pytest.param("SELECT\n  1;", ["SELECT\n  1"], id="keeps-inner-newline"),
    ],
)
def test_basic_splitting(script: str, expected: list[str]) -> None:
    assert split_sql_statements(script) == expected


@pytest.mark.parametrize(
    ("script", "expected"),
    [
        pytest.param("SELECT 'a;b';", ["SELECT 'a;b'"], id="single-quote"),
        pytest.param('SELECT "a;b";', ['SELECT "a;b"'], id="double-quote"),
        pytest.param("SELECT `a;b`;", ["SELECT `a;b`"], id="backtick"),
        pytest.param("SELECT 'it''s;';SELECT 2", ["SELECT 'it''s;'", "SELECT 2"], id="doubled"),
        pytest.param(r"SELECT 'it\'s;';SELECT 2", [r"SELECT 'it\'s;'", "SELECT 2"], id="backslash"),
        pytest.param(r"SELECT 'x\\';SELECT 2", [r"SELECT 'x\\'", "SELECT 2"], id="escaped-backslash"),
        pytest.param('SELECT "a""b;";', ['SELECT "a""b;"'], id="doubled-double-quote"),
        pytest.param("SELECT `a``b;`;", ["SELECT `a``b;`"], id="doubled-backtick"),
        pytest.param(r"SELECT `a\`;SELECT 2", [r"SELECT `a\`", "SELECT 2"], id="backtick-no-backslash"),
        pytest.param("SELECT '-- no';", ["SELECT '-- no'"], id="dash-in-string"),
        pytest.param("SELECT '# no';", ["SELECT '# no'"], id="hash-in-string"),
        pytest.param("SELECT '/* no */';", ["SELECT '/* no */'"], id="block-in-string"),
        pytest.param("SELECT \"it's;\";", ["SELECT \"it's;\""], id="other-quote-inside"),
    ],
)
def test_quoted_text_is_left_alone(script: str, expected: list[str]) -> None:
    assert split_sql_statements(script) == expected


@pytest.mark.parametrize(
    ("script", "expected"),
    [
        pytest.param("-- a; b\nSELECT 1;", ["SELECT 1"], id="dash-comment"),
        pytest.param("SELECT 1 -- a; b\n;", ["SELECT 1"], id="dash-after-code"),
        pytest.param("# a; b\nSELECT 1;", ["SELECT 1"], id="hash-comment"),
        pytest.param("/* a; b */SELECT 1;", ["SELECT 1"], id="block-comment"),
        pytest.param("/* a;\n b */\nSELECT 1;", ["SELECT 1"], id="multi-line-block"),
        pytest.param("-- it's\nSELECT 1;", ["SELECT 1"], id="apostrophe-in-dash-comment"),
        pytest.param("# it's\nSELECT 1;", ["SELECT 1"], id="apostrophe-in-hash-comment"),
        pytest.param("/* it's */SELECT 1;", ["SELECT 1"], id="apostrophe-in-block-comment"),
        pytest.param("/* -- */SELECT 1;", ["SELECT 1"], id="dash-in-block-comment"),
        pytest.param("-- /* no\nSELECT 1;", ["SELECT 1"], id="block-open-in-dash-comment"),
        pytest.param("-- only a comment", [], id="comment-only"),
        pytest.param("--", [], id="bare-dash-at-end"),
        pytest.param("--\nSELECT 1;", ["SELECT 1"], id="bare-dash-before-newline"),
        pytest.param("--\tx\nSELECT 1;", ["SELECT 1"], id="dash-before-tab"),
        pytest.param("-- x\r\nSELECT 1;\r\n", ["SELECT 1"], id="windows-newlines"),
        pytest.param("/* a */ ; /* b */ ;", [], id="statements-of-only-comments"),
        pytest.param("SELECT 5--1;", ["SELECT 5--1"], id="double-minus-is-not-a-comment"),
        pytest.param("SELECT 1/*x*/+2;", ["SELECT 1 +2"], id="comment-separates-tokens"),
        pytest.param("SELECT 6/2;", ["SELECT 6/2"], id="division-is-not-a-comment"),
    ],
)
def test_comments_are_removed(script: str, expected: list[str]) -> None:
    assert split_sql_statements(script) == expected


@pytest.mark.parametrize(
    "script",
    [
        pytest.param("SELECT /*!40001 SQL_NO_CACHE */ 1", id="version-comment"),
        pytest.param("SELECT /*+ NO_INDEX(t) */ 1", id="optimizer-hint"),
    ],
)
def test_comments_the_server_executes_are_kept(script: str) -> None:
    assert split_sql_statements(script + ";") == [script]


@pytest.mark.parametrize(
    ("script", "message"),
    [
        pytest.param("SELECT 'oops", "Unterminated '", id="single"),
        pytest.param('SELECT "oops', 'Unterminated "', id="double"),
        pytest.param("SELECT `oops", "Unterminated `", id="backtick"),
        pytest.param("SELECT 'oops\\'", "Unterminated '", id="escaped-closing-quote"),
        pytest.param("SELECT 1;\n/* oops", "Unterminated comment on line 2", id="block"),
    ],
)
def test_truncated_script_is_refused(script: str, message: str) -> None:
    with pytest.raises(SqlScriptError, match=re.escape(message)):
        split_sql_statements(script)


@pytest.fixture(scope="module")
def script() -> str:
    return locate_create_dw_script().read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def statements(script: str) -> list[str]:
    return split_sql_statements(script)


class TestCreateDwScript:
    """Static shape of the graded script. The integration tests run it for real."""

    def test_first_statement_drops_the_view(self, statements: list[str]) -> None:
        assert statements[0] == "DROP VIEW IF EXISTS STORE_QUARTERLY_SALES"

    def test_drop_order_follows_dependencies(self, statements: list[str]) -> None:
        drops = [s.split()[-1] for s in statements if s.startswith("DROP TABLE")]
        assert drops[0] == "fact_sales"
        assert set(drops[1:6]) == {
            "dim_customer",
            "dim_product",
            "dim_supplier",
            "dim_store",
            "dim_time",
        }

    def test_every_created_table_is_dropped_first(self, statements: list[str]) -> None:
        created = [s.split()[2] for s in statements if s.startswith("CREATE TABLE")]
        dropped = [s.split()[-1] for s in statements if s.startswith("DROP TABLE")]
        assert sorted(created) == sorted(dropped)
        last_drop = max(i for i, s in enumerate(statements) if s.startswith("DROP"))
        first_create = min(i for i, s in enumerate(statements) if s.startswith("CREATE"))
        assert last_drop < first_create

    def test_creation_order_is_dimensions_then_fact_then_working(
        self, statements: list[str]
    ) -> None:
        created = [s.split()[2] for s in statements if s.startswith("CREATE TABLE")]
        assert all(name.startswith("dim_") for name in created[:5])
        assert created[5] == "fact_sales"
        assert not any(name.startswith("dim_") for name in created[6:])

    def test_caller_selects_the_database(self, statements: list[str]) -> None:
        for statement in statements:
            assert not re.match(r"(USE|CREATE\s+DATABASE)\b", statement, re.IGNORECASE)

    def test_needs_no_delimiter_or_stored_programs(self, statements: list[str]) -> None:
        pattern = r"\b(DELIMITER|PROCEDURE|FUNCTION|TRIGGER)\b"
        assert not [s for s in statements if re.search(pattern, s, re.IGNORECASE)]

    def test_every_table_is_innodb_and_utf8mb4(self, statements: list[str]) -> None:
        for statement in (s for s in statements if s.startswith("CREATE TABLE")):
            assert "ENGINE = InnoDB" in statement
            assert "CHARSET = utf8mb4" in statement

    def test_banners_explain_the_model_and_the_grain(self, script: str) -> None:
        for phrase in ("Section 1", "Section 2", "Section 3", "Section 4", "GRAIN:", "star"):
            assert phrase in script
