"""The independent rules must reproduce the assignment's rules and the reference totals,
otherwise the oracle and the star fixture would be checking against something wrong."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from tests.oracle.independent_rules import id_as_text, is_online_cart, md5_prefix, store_of_cart


@pytest.mark.parametrize(
    ("invoice", "number", "online", "store"),
    [("536365", 1047349710, True, 1), ("536370", 764514097, False, 3)],
)
def test_md5_rule_vectors(invoice: str, number: int, online: bool, store: int) -> None:
    assert md5_prefix(invoice) == number
    assert is_online_cart(invoice) is online
    assert store_of_cart(invoice) == store


@pytest.mark.parametrize(
    ("value", "text"),
    [(17850.0, "17850"), (71053, "71053"), ("85123A", "85123A"), (float("nan"), None), (None, None)],
)
def test_ids_become_canonical_text(value: object, text: str | None) -> None:
    assert id_as_text(value) == text


@pytest.mark.integration
class TestReferenceTotals:
    """Spec section 4, measured on the real file. Marked integration because reading the
    workbook takes about a minute and this only matters when the harness will be used."""

    def test_row_and_money_totals(self, raw_frame: pd.DataFrame) -> None:
        assert len(raw_frame) == 397_884
        assert raw_frame["quantity"].sum() == 5_167_812
        assert raw_frame["revenue_mills"].sum() == 8_911_407_904
        assert raw_frame["transaction_id"].nunique() == 18_532

    def test_sales_mode_split(self, raw_frame: pd.DataFrame) -> None:
        groups = raw_frame.groupby("sales_mode").agg(
            rows=("quantity", "size"), revenue=("revenue_mills", "sum")
        )
        assert groups.loc["Online"].tolist() == [278_740, 6_075_186_073]
        assert groups.loc["Periodic"].tolist() == [119_144, 2_836_221_831]

    def test_store_split(self, raw_frame: pd.DataFrame) -> None:
        groups = raw_frame.groupby("store_id").agg(
            rows=("quantity", "size"), revenue=("revenue_mills", "sum")
        )
        assert groups["rows"].tolist() == [278_740, 21_053, 37_302, 19_850, 40_939]
        assert groups["revenue"].tolist() == [
            6_075_186_073, 463_139_470, 868_061_800, 632_043_680, 872_976_881,
        ]

    def test_year_split(self, raw_frame: pd.DataFrame) -> None:
        groups = raw_frame.groupby("year_no").agg(
            rows=("quantity", "size"), revenue=("revenue_mills", "sum")
        )
        assert groups.loc[2010].tolist() == [26_157, 572_713_890]
        assert groups.loc[2011].tolist() == [371_727, 8_338_694_014]

    def test_dimension_cardinalities(self, raw_frame: pd.DataFrame) -> None:
        assert raw_frame["customer_id"].nunique() == 4_338
        assert raw_frame["product_id"].nunique() == 3_665
        assert raw_frame["supplier_id"].nunique() == 5
        assert raw_frame["store_id"].nunique() == 5
        assert raw_frame["time_key"].nunique() == 17_282

    def test_dates_named_in_the_query_script(self, raw_frame: pd.DataFrame) -> None:
        assert raw_frame["event_time"].min() == datetime(2010, 12, 1, 8, 26)
        assert raw_frame["event_time"].max() == datetime(2011, 12, 9, 12, 50)

    def test_time_key_is_the_full_timestamp(self, raw_frame: pd.DataFrame) -> None:
        """A count of distinct keys cannot see an integer overflow, so check the values."""
        assert raw_frame["time_key"].min() == 201012010826
        assert raw_frame["time_key"].max() == 201112091250

    def test_every_cart_has_one_customer_and_one_store(self, raw_frame: pd.DataFrame) -> None:
        carts = raw_frame.groupby("transaction_id")
        assert carts["customer_id"].nunique().max() == 1
        assert carts["store_id"].nunique().max() == 1

    def test_the_file_has_no_saturday_sales(self, raw_frame: pd.DataFrame) -> None:
        """Q10 states this in a comment, so the comment is checked here."""
        assert not (raw_frame["day_of_week"] == 6).any()
        assert set(raw_frame.loc[raw_frame["day_type"] == "Weekend", "day_name"]) == {"Sunday"}
