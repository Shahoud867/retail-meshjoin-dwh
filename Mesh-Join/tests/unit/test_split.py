"""Marking carts, the online file, and the 48 periodic groups."""

from __future__ import annotations

import csv
import itertools
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from retail_dw.config import PERIODIC_GROUPS
from retail_dw.meshjoin.model import SALES_MODE_ONLINE, SALES_MODE_PERIODIC
from retail_dw.prepare.rules import sales_mode_for
from retail_dw.prepare.split import (
    ONLINE_COLUMNS,
    build_periodic_groups,
    group_sizes,
    mark_sales_mode,
    write_online_file,
)

ONLINE_CART = "536365"
PERIODIC_CART = "536370"
START = datetime(2011, 1, 1, 9, 0)


def carts_of(mode: str, count: int) -> list[str]:
    candidates = (str(number) for number in itertools.count(536000))
    return list(itertools.islice((c for c in candidates if sales_mode_for(c) == mode), count))


def cleaned_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    """A frame shaped like clean()'s output, filling in columns a test does not care about."""
    filled = [
        {
            "transaction_id": PERIODIC_CART,
            "customer_id": "17850",
            "product_id": "85123A",
            "product_name": "HEART",
            "quantity": 1,
            "unit_price": Decimal("2.55"),
            "event_time": START,
            "country": "France",
            **row,
        }
        for row in rows
    ]
    frame = pd.DataFrame(filled)
    return frame.astype({"transaction_id": object, "unit_price": object})


def periodic_frame(count: int) -> pd.DataFrame:
    rows = [{"product_id": f"P{index}"} for index in range(count)]
    return mark_sales_mode(cleaned_frame(rows))


# --- mark_sales_mode ---------------------------------------------------------------


def test_mode_comes_from_the_invoice_rule() -> None:
    marked = mark_sales_mode(
        cleaned_frame([{"transaction_id": ONLINE_CART}, {"transaction_id": PERIODIC_CART}])
    )
    assert marked["sales_mode"].tolist() == [SALES_MODE_ONLINE, SALES_MODE_PERIODIC]


def test_every_line_of_a_cart_gets_one_mode() -> None:
    rows = [{"transaction_id": ONLINE_CART, "product_id": f"P{n}"} for n in range(5)]
    rows += [{"transaction_id": PERIODIC_CART, "product_id": f"P{n}"} for n in range(5)]
    marked = mark_sales_mode(cleaned_frame(rows))
    assert marked.groupby("transaction_id")["sales_mode"].nunique().max() == 1


def test_marking_returns_a_copy_and_keeps_rows() -> None:
    frame = cleaned_frame([{"transaction_id": ONLINE_CART}])
    marked = mark_sales_mode(frame)
    assert "sales_mode" not in frame.columns
    assert len(marked) == 1


def test_marking_an_empty_frame_works() -> None:
    frame = cleaned_frame([{}]).iloc[0:0]
    assert mark_sales_mode(frame).empty


# --- online file -------------------------------------------------------------------


def test_online_file_has_the_exact_columns_and_only_online_rows(tmp_path: Path) -> None:
    path = tmp_path / "online.csv"
    marked = mark_sales_mode(
        cleaned_frame(
            [
                {
                    "transaction_id": ONLINE_CART,
                    "customer_id": "17850",
                    "product_id": "71053",
                    "quantity": 6,
                    "unit_price": Decimal("3.39"),
                    "event_time": datetime(2010, 12, 1, 8, 26),
                },
                {"transaction_id": PERIODIC_CART},
            ]
        )
    )
    assert write_online_file(marked, path) == 1
    assert path.read_bytes() == (
        b"transaction_id,customer_id,product_id,quantity,unit_price,event_time,sales_mode\n"
        b"536365,17850,71053,6,3.39,2010-12-01 08:26:00,Online\n"
    )
    assert ONLINE_COLUMNS == (
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "event_time",
        "sales_mode",
    )


def test_online_file_keeps_file_order_not_time_order(tmp_path: Path) -> None:
    """The resume offset counts lines of this file, so its order may never change."""
    path = tmp_path / "online.csv"
    rows = [
        {
            "transaction_id": ONLINE_CART,
            "product_id": f"P{index}",
            "event_time": START - timedelta(minutes=index),
        }
        for index in range(5)
    ]
    write_online_file(mark_sales_mode(cleaned_frame(rows)), path)
    with path.open(newline="", encoding="utf-8") as handle:
        assert [row["product_id"] for row in csv.DictReader(handle)] == [
            "P0",
            "P1",
            "P2",
            "P3",
            "P4",
        ]


def test_online_file_price_text_round_trips_exactly(tmp_path: Path) -> None:
    path = tmp_path / "online.csv"
    prices = ["0.001", "1.0", "2.55", "8142.75", "16.95"]
    rows = [
        {"transaction_id": ONLINE_CART, "unit_price": Decimal(price)} for price in prices
    ]
    write_online_file(mark_sales_mode(cleaned_frame(rows)), path)
    with path.open(newline="", encoding="utf-8") as handle:
        assert [row["unit_price"] for row in csv.DictReader(handle)] == prices


def test_online_file_quotes_awkward_ids_lf_only_utf8(tmp_path: Path) -> None:
    path = tmp_path / "deep" / "er" / "online.csv"
    rows = [{"transaction_id": ONLINE_CART, "product_id": 'A,"B"É'}]
    write_online_file(mark_sales_mode(cleaned_frame(rows)), path)
    data = path.read_bytes()
    assert b"\r" not in data
    assert not data.startswith(b"\xef\xbb\xbf")
    with path.open(newline="", encoding="utf-8") as handle:
        assert next(csv.DictReader(handle))["product_id"] == 'A,"B"É'


def test_online_file_with_no_online_rows_has_only_the_header(tmp_path: Path) -> None:
    path = tmp_path / "online.csv"
    assert write_online_file(mark_sales_mode(cleaned_frame([{}])), path) == 0
    assert path.read_text(encoding="utf-8").count("\n") == 1


# --- group sizes -------------------------------------------------------------------


@pytest.mark.parametrize("total", [0, 1, 7, 47, 48, 49, 55, 95, 96, 97, 1000, 119144])
def test_group_sizes_match_numpy_array_split(total: int) -> None:
    expected = [len(part) for part in np.array_split(np.arange(total), PERIODIC_GROUPS)]
    assert group_sizes(total) == expected


def test_real_total_gives_the_reference_sizes() -> None:
    assert group_sizes(119144) == [2483] * 8 + [2482] * 40


def test_first_groups_hold_the_extra_rows() -> None:
    assert group_sizes(50) == [2, 2] + [1] * 46


# --- periodic groups ---------------------------------------------------------------


def test_groups_are_always_forty_eight_in_release_order() -> None:
    groups = build_periodic_groups(periodic_frame(100))
    assert len(groups) == 48
    assert [len(group) for group in groups] == [3, 3, 3, 3] + [2] * 44
    for release_hour, group in enumerate(groups):
        assert {row[7] for row in group} == {release_hour}


def test_fewer_rows_than_groups_leaves_the_later_groups_empty() -> None:
    groups = build_periodic_groups(periodic_frame(3))
    assert [len(group) for group in groups] == [1, 1, 1] + [0] * 45


def test_no_periodic_rows_gives_forty_eight_empty_groups() -> None:
    frame = mark_sales_mode(cleaned_frame([{"transaction_id": ONLINE_CART}]))
    assert build_periodic_groups(frame) == [[] for _ in range(48)]


def test_an_empty_frame_gives_forty_eight_empty_groups() -> None:
    frame = mark_sales_mode(cleaned_frame([{}]).iloc[0:0])
    assert build_periodic_groups(frame) == [[] for _ in range(48)]


def test_groups_hold_only_periodic_rows() -> None:
    online_carts = carts_of(SALES_MODE_ONLINE, 10)
    periodic_carts = carts_of(SALES_MODE_PERIODIC, 10)
    rows = [{"transaction_id": cart} for cart in online_carts + periodic_carts]
    groups = build_periodic_groups(mark_sales_mode(cleaned_frame(rows)))
    flat = list(itertools.chain.from_iterable(groups))
    assert len(flat) == 10
    assert {row[0] for row in flat} == set(periodic_carts)
    assert {row[6] for row in flat} == {SALES_MODE_PERIODIC}


def test_rows_are_sorted_by_event_time_across_groups() -> None:
    rows = [
        {"product_id": f"P{index}", "event_time": START + timedelta(minutes=(index * 37) % 96)}
        for index in range(96)
    ]
    groups = build_periodic_groups(mark_sales_mode(cleaned_frame(rows)))
    flat = list(itertools.chain.from_iterable(groups))
    times = [row[5] for row in flat]
    assert times == sorted(times)
    assert [len(group) for group in groups] == [2] * 48


def test_sort_is_stable_for_rows_in_the_same_minute() -> None:
    """Without a stable sort the group boundaries could differ from one run to another."""
    rows = [
        {"product_id": f"LATE{index}", "event_time": START + timedelta(minutes=1)}
        for index in range(50)
    ] + [{"product_id": f"EARLY{index}", "event_time": START} for index in range(50)]
    groups = build_periodic_groups(mark_sales_mode(cleaned_frame(rows)))
    flat = [row[2] for row in itertools.chain.from_iterable(groups)]
    assert flat == [f"EARLY{index}" for index in range(50)] + [f"LATE{index}" for index in range(50)]


def test_ties_across_a_group_boundary_stay_in_file_order() -> None:
    rows = [{"product_id": f"P{index:03}"} for index in range(200)]
    groups = build_periodic_groups(mark_sales_mode(cleaned_frame(rows)))
    flat = [row[2] for row in itertools.chain.from_iterable(groups)]
    assert flat == sorted(flat)
    assert groups[0][-1][2] == "P004"
    assert groups[1][0][2] == "P005"


def test_a_cart_may_be_split_by_a_group_boundary() -> None:
    """Groups are equal counts, so one cart can span two of them (see the ReadMe)."""
    rows = [{"product_id": f"P{index}"} for index in range(96)]
    groups = build_periodic_groups(mark_sales_mode(cleaned_frame(rows)))
    assert {row[0] for row in groups[0]} == {row[0] for row in groups[47]} == {PERIODIC_CART}


def test_staging_row_values_are_plain_python_in_column_order() -> None:
    frame = mark_sales_mode(
        cleaned_frame(
            [
                {
                    "transaction_id": PERIODIC_CART,
                    "customer_id": "12583",
                    "product_id": "22728",
                    "quantity": 24,
                    "unit_price": Decimal("3.75"),
                    "event_time": datetime(2010, 12, 1, 8, 45),
                }
            ]
        )
    )
    row = build_periodic_groups(frame)[0][0]
    assert row == (
        "536370",
        "12583",
        "22728",
        24,
        Decimal("3.75"),
        datetime(2010, 12, 1, 8, 45),
        "Periodic",
        0,
    )
    assert [type(value) for value in row] == [str, str, str, int, Decimal, datetime, str, int]
