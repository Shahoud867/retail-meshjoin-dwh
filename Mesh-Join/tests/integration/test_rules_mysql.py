"""The Python rules checked once against MySQL's own functions.

The server's date functions and MD5 are a second opinion that shares no code with
rules.py, so an off-by-one in a weekday or a season cannot pass on both sides.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from retail_dw.db import Database
from retail_dw.prepare.rules import md5_number, sales_mode_for, stable, store_id_for, time_parts

pytestmark = pytest.mark.integration

_PARTS_FROM_MYSQL = """
SELECT
    CAST(DATE_FORMAT(moment, %s) AS UNSIGNED),
    moment,
    DATE(moment),
    HOUR(moment),
    DAYOFMONTH(moment),
    MONTH(moment),
    MONTHNAME(moment),
    QUARTER(moment),
    YEAR(moment),
    WEEKDAY(moment) + 1,
    DAYNAME(moment),
    CASE WHEN WEEKDAY(moment) >= 5 THEN 'Weekend' ELSE 'Weekday' END,
    CASE
        WHEN MONTH(moment) BETWEEN 3 AND 5 THEN 'Spring'
        WHEN MONTH(moment) BETWEEN 6 AND 8 THEN 'Summer'
        WHEN MONTH(moment) BETWEEN 9 AND 11 THEN 'Fall'
        ELSE 'Winter'
    END,
    CASE WHEN MONTH(moment) <= 6 THEN 'H1' ELSE 'H2' END
FROM (SELECT CAST(%s AS DATETIME) AS moment) AS given
"""


def moments_to_check() -> list[datetime]:
    """The first and last minute of every day of the loaded period's calendar, sampled
    so that every month edge, every weekday and both years are covered."""
    moments = []
    day = date(2010, 12, 1)
    while day <= date(2011, 12, 31):
        moments.append(datetime(day.year, day.month, day.day, 0, 0))
        moments.append(datetime(day.year, day.month, day.day, 23, 59))
        day += timedelta(days=1)
    return moments


def test_time_parts_match_mysql_for_every_day_of_the_period(db: Database) -> None:
    db.execute("SET lc_time_names = 'en_US'")
    for moment in moments_to_check():
        [mysql_row] = db.query(_PARTS_FROM_MYSQL, ("%Y%m%d%H%i", moment))
        assert tuple(time_parts(moment).values()) == mysql_row, moment


def test_time_parts_fit_the_dim_time_table(clean_db: Database) -> None:
    """The types and CHECK constraints accept the rows, and read back unchanged."""
    moments = moments_to_check()
    columns = list(time_parts(moments[0]))
    insert = (
        f"INSERT INTO dim_time ({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(columns))})"
    )
    clean_db.executemany(insert, [tuple(time_parts(moment).values()) for moment in moments])
    rows = clean_db.query(f"SELECT {', '.join(columns)} FROM dim_time ORDER BY time_key")
    assert rows == [tuple(time_parts(moment).values()) for moment in moments]


@pytest.mark.parametrize(
    "text",
    ["536365", "536370", "C536379", "17850age", "17850United Kingdom", "85123Acat", "café"],
)
def test_md5_number_matches_mysql(db: Database, text: str) -> None:
    [(number,)] = db.query("SELECT CONV(LEFT(MD5(CONVERT(%s USING utf8mb4)), 8), 16, 10)", (text,))
    assert md5_number(text) == int(number)


def test_cart_rules_match_mysql_on_a_range_of_invoices(db: Database) -> None:
    for number in range(536365, 536465):
        text = str(number)
        [(remainder, store)] = db.query(
            "SELECT CONV(LEFT(MD5(%s), 8), 16, 10) MOD 10, "
            "2 + CONV(LEFT(MD5(%s), 8), 16, 10) MOD 4",
            (text, text),
        )
        mode = "Online" if int(remainder) <= 6 else "Periodic"
        assert sales_mode_for(text) == mode
        assert store_id_for(text, "Periodic") == int(store)
        assert stable(text, 10) == int(remainder)
