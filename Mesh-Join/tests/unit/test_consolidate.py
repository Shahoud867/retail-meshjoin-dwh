"""Consolidation: revenue exactness, the store rule and the Master Data tuple order."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime
from decimal import Decimal, Inexact
from fractions import Fraction

import pytest

from retail_dw.load.consolidate import EnrichedRow, consolidate, rebuild_row
from retail_dw.meshjoin.model import (
    PROCESSING_LIVE,
    PROCESSING_PERIODIC,
    PROCESSING_SHED,
    SALES_MODE_ONLINE,
    SALES_MODE_PERIODIC,
    Transaction,
)
from retail_dw.prepare.rules import STORE_LIST, time_parts

# In the table column order pinned by the spec addendum. Every value is different, so
# reading a position wrongly cannot go unnoticed.
CUSTOMER = ("17850", "Male", "26-35", "Engineer", "B", "4+", "Married")
PRODUCT = ("85123A", "WHITE HANGING HEART", "Home Decor", "SUP03", "Midlands Wholesale")
EVENT_TIME = datetime(2010, 12, 1, 8, 26)


def _transaction(
    quantity: int = 6,
    unit_price: Decimal = Decimal("2.55"),
    transaction_id: str = "536365",
    sales_mode: str = SALES_MODE_ONLINE,
    processing_mode: str = PROCESSING_LIVE,
    staging_id: int | None = None,
    customer: tuple | None = CUSTOMER,
    product: tuple | None = PRODUCT,
) -> Transaction:
    tx = Transaction(
        transaction_id=transaction_id,
        customer_id=CUSTOMER[0],
        product_id=PRODUCT[0],
        quantity=quantity,
        unit_price=unit_price,
        event_time=EVENT_TIME,
        sales_mode=sales_mode,
        processing_mode=processing_mode,
        staging_id=staging_id,
    )
    tx.customer = customer
    tx.product = product
    return tx


class TestRevenue:
    def test_largest_real_line_is_exact_to_three_places(self) -> None:
        row = consolidate(_transaction(quantity=80995, unit_price=Decimal("2.08")))
        assert str(row.revenue) == "168469.600"

    @pytest.mark.parametrize(
        ("quantity", "price", "expected"),
        [
            (1, "0.001", "0.001"),
            (3, "0.001", "0.003"),
            (1000, "0.001", "1.000"),
            (7, "1.1", "7.700"),
            (6, "2.55", "15.300"),
            (3, "0.415", "1.245"),
            (74215, "1.04", "77183.600"),
        ],
    )
    def test_exact_values(self, quantity: int, price: str, expected: str) -> None:
        row = consolidate(_transaction(quantity=quantity, unit_price=Decimal(price)))
        assert str(row.revenue) == expected

    def test_smallest_price_does_not_round_to_zero(self) -> None:
        assert consolidate(_transaction(1, Decimal("0.001"))).revenue > 0

    def test_matches_exact_rational_arithmetic(self) -> None:
        """Binary floats get values like these wrong, so a float anywhere would show."""
        for quantity in (1, 3, 7, 12, 99, 1001, 80995):
            for thousandths in (1, 7, 29, 415, 1250, 2080, 8142, 38970):
                price = Decimal(thousandths) / 1000
                revenue = consolidate(_transaction(quantity, price)).revenue
                assert Fraction(revenue) == Fraction(quantity * thousandths, 1000)
                assert revenue.as_tuple().exponent == -3

    def test_refuses_a_price_the_warehouse_cannot_hold_exactly(self) -> None:
        with pytest.raises(Inexact):
            consolidate(_transaction(1, Decimal("0.0005")))

    def test_refuses_a_float_price(self) -> None:
        with pytest.raises(TypeError):
            consolidate(_transaction(1, 2.08))  # type: ignore[arg-type]


class TestStoreRule:
    def test_online_transaction_is_the_web_store(self) -> None:
        row = consolidate(_transaction())
        assert (row.store_id, row.store_name, row.city) == (1, "Online", "Web")

    def test_periodic_transaction_uses_the_md5_store(self) -> None:
        """Spec test vector: 536370 has MD5 number 764514097, % 4 = 1, so store 3."""
        row = consolidate(
            _transaction(
                transaction_id="536370",
                sales_mode=SALES_MODE_PERIODIC,
                processing_mode=PROCESSING_PERIODIC,
                staging_id=9,
            )
        )
        assert (row.store_id, row.store_name, row.city) == (
            3,
            "Manchester Arndale",
            "Manchester",
        )

    def test_store_attributes_always_come_from_store_list(self) -> None:
        for number in range(536365, 536465):
            row = consolidate(
                _transaction(
                    transaction_id=str(number),
                    sales_mode=SALES_MODE_PERIODIC,
                    processing_mode=PROCESSING_PERIODIC,
                    staging_id=1,
                )
            )
            assert row.store_id in {2, 3, 4, 5}
            assert (row.store_name, row.city) == STORE_LIST[row.store_id]


class TestTupleOrder:
    def test_customer_tuple_is_read_in_table_column_order(self) -> None:
        row = consolidate(_transaction())
        assert (
            row.customer_id,
            row.gender,
            row.age_group,
            row.occupation,
            row.city_category,
            row.stay_in_current_city_years,
            row.marital_status,
        ) == CUSTOMER

    def test_product_tuple_is_read_in_table_column_order(self) -> None:
        row = consolidate(_transaction())
        assert (
            row.product_id,
            row.product_name,
            row.product_category,
            row.supplier_id,
            row.supplier_name,
        ) == PRODUCT

    def test_tuple_of_the_wrong_length_is_refused(self) -> None:
        with pytest.raises(ValueError):
            consolidate(_transaction(customer=CUSTOMER[:-1]))


class TestCopiedFields:
    def test_measures_and_lineage_are_unchanged(self) -> None:
        row = consolidate(
            _transaction(
                quantity=2,
                transaction_id="536370",
                sales_mode=SALES_MODE_PERIODIC,
                processing_mode=PROCESSING_SHED,
                staging_id=41,
            )
        )
        assert row.transaction_id == "536370"
        assert row.quantity == 2
        assert row.sales_mode == SALES_MODE_PERIODIC
        assert row.processing_mode == PROCESSING_SHED
        assert row.staging_id == 41
        assert row.event_time == EVENT_TIME

    def test_time_parts_are_computed_from_the_event_time(self) -> None:
        row = consolidate(_transaction())
        assert row.time == time_parts(EVENT_TIME)
        assert row.time["time_key"] == 201012010826

    def test_row_cannot_be_changed_after_consolidation(self) -> None:
        with pytest.raises(FrozenInstanceError):
            consolidate(_transaction()).revenue = Decimal(1)  # type: ignore[misc]


class TestUnmatched:
    @pytest.mark.parametrize(
        ("customer", "product"),
        [(None, PRODUCT), (CUSTOMER, None), (None, None)],
    )
    def test_unmatched_transaction_is_never_consolidated(
        self, customer: tuple | None, product: tuple | None
    ) -> None:
        with pytest.raises(ValueError, match="unmatched"):
            consolidate(_transaction(customer=customer, product=product))


class TestRebuild:
    @staticmethod
    def _stored(revenue: Decimal) -> dict:
        row = consolidate(_transaction())
        fields = (
            "transaction_id customer_id gender age_group occupation city_category "
            "stay_in_current_city_years marital_status product_id product_name "
            "product_category supplier_id supplier_name store_id event_time quantity "
            "sales_mode processing_mode"
        ).split()
        return {name: getattr(row, name) for name in fields} | {"revenue": revenue}

    def test_rebuilt_row_equals_the_original(self) -> None:
        original = consolidate(_transaction())
        rebuilt = rebuild_row(self._stored(original.revenue))
        assert isinstance(rebuilt, EnrichedRow)
        assert rebuilt == original

    def test_stored_revenue_is_kept_and_not_computed_again(self) -> None:
        marker = Decimal("123.456")
        assert rebuild_row(self._stored(marker)).revenue == marker
