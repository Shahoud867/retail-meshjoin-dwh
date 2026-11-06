"""The expected answer to every question, computed in pandas from the raw file.

No SQL is involved, so a mistake in a query cannot also be a mistake here. Money is summed
as integer thousandths and converted to Decimal only at the end, so the comparison with
MySQL can be exact to the last digit, with no tolerance.

Each function returns rows in the order the matching query is required to return them.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction

import numpy as np
import pandas as pd

LOADED_YEAR = 2011

# MySQL keeps a division of a three-decimal value at 3 + div_precision_increment (4) = 7
# decimals, rounding half away from zero, and ROUND(x, n) then rounds that value again.
# The second rounding differs from a single exact one for about one row in 2,000 of Q13,
# so a comparison to the last digit has to follow the same two steps.
_MYSQL_DIVISION_DECIMALS = 7


@dataclass(frozen=True)
class Answer:
    columns: tuple[str, ...]
    rows: list[tuple]


def money(mills: int) -> Decimal:
    """Thousandths as a Decimal with three decimals, the type of the revenue column."""
    return Decimal(int(mills)).scaleb(-3)


def _half_up(value: Fraction, places: int) -> Decimal:
    """Round half away from zero, which is what MySQL does with exact numbers."""
    scaled = abs(value) * 10**places
    whole = (2 * scaled.numerator + scaled.denominator) // (2 * scaled.denominator)
    return Decimal(whole if value >= 0 else -whole).scaleb(-places)


def _rounded_division(value: Fraction, places: int) -> Decimal:
    """ROUND(a / b, places) as MySQL evaluates it. See _MYSQL_DIVISION_DECIMALS."""
    return _half_up(Fraction(_half_up(value, _MYSQL_DIVISION_DECIMALS)), places)


def _percent_change(current: int, previous: int | None) -> Decimal | None:
    """Percentage change to two decimals, or NULL when there is no usable base."""
    if previous is None or previous == 0:
        return None
    return _rounded_division(Fraction(100 * (current - previous), previous), 2)


def _sum_revenue(frame: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    return frame.groupby(by, as_index=False, sort=True)["revenue_mills"].sum()


def _top_n(
    groups: pd.DataFrame, partition: list[str], tie_break: str, rank_column: str, n: int
) -> pd.DataFrame:
    """Rank by revenue inside each partition with a deterministic tie break, keep n."""
    ordered = groups.sort_values(
        [*partition, "revenue_mills", tie_break],
        ascending=[True] * len(partition) + [False, True],
        kind="stable",
    )
    ranked = ordered.assign(**{rank_column: ordered.groupby(partition).cumcount() + 1})
    return ranked[ranked[rank_column] <= n]


def _with_previous(monthly: pd.DataFrame, partition: list[str], order: list[str]) -> pd.DataFrame:
    """LAG, done before any filtering. shift() returns floats with NaN for the first row
    of a group, and revenues of this size survive that exactly, so it converts back."""
    ordered = monthly.sort_values([*partition, *order], kind="stable")
    previous = ordered.groupby(partition)["revenue_mills"].shift(1)
    return ordered.assign(previous_mills=previous.astype("Int64"))


def _optional_int(value: object) -> int | None:
    return None if pd.isna(value) else int(value)


def _optional_money(mills: int | None) -> Decimal | None:
    return None if mills is None else money(mills)


def _conditional_sum(frame: pd.DataFrame, column: str, value: str, measure: str) -> pd.Series:
    """The measure where the column has this value, zero elsewhere: SUM(CASE ...) in SQL."""
    return frame[measure].where(frame[column] == value, 0)


def q1(frame: pd.DataFrame) -> Answer:
    year = frame[frame["year_no"] == LOADED_YEAR]
    totals = _sum_revenue(year, ["day_type", "product_id"])
    top = _top_n(totals, ["day_type"], "product_id", "product_rank", 5).rename(
        columns={"revenue_mills": "year_mills"}
    )
    detail = year.merge(top, on=["day_type", "product_id"])
    monthly = _sum_revenue(
        detail,
        ["day_type", "product_rank", "product_id", "product_name", "month_no", "month_name",
         "year_mills"],
    ).sort_values(["day_type", "product_rank", "month_no"])
    return Answer(
        ("day_type", "product_rank", "product_id", "product_name", "month_no", "month_name",
         "month_revenue", "year_revenue"),
        [
            (r.day_type, r.product_rank, r.product_id, r.product_name, r.month_no, r.month_name,
             money(r.revenue_mills), money(r.year_mills))
            for r in monthly.itertuples()
        ],
    )


def q2(frame: pd.DataFrame) -> Answer:
    groups = _sum_revenue(frame, ["gender", "age_group", "city_category"])
    return Answer(
        ("gender", "age_group", "city_category", "total_purchase_amount"),
        [
            (r.gender, r.age_group, r.city_category, money(r.revenue_mills))
            for r in groups.itertuples()
        ],
    )


def q3(frame: pd.DataFrame) -> Answer:
    groups = _sum_revenue(frame, ["product_category", "occupation"]).sort_values(
        ["product_category", "revenue_mills", "occupation"], ascending=[True, False, True]
    )
    return Answer(
        ("product_category", "occupation", "total_sales"),
        [(r.product_category, r.occupation, money(r.revenue_mills)) for r in groups.itertuples()],
    )


def q4(frame: pd.DataFrame) -> Answer:
    year = frame[frame["year_no"] == LOADED_YEAR]
    pivot = year.pivot_table(
        index=["gender", "age_group"],
        columns="quarter_no",
        values="revenue_mills",
        aggfunc="sum",
        fill_value=0,
    ).sort_index()
    rows = []
    for (gender, age_group), quarters in pivot.iterrows():
        values = [int(quarters.get(quarter, 0)) for quarter in (1, 2, 3, 4)]
        rows.append((gender, age_group, *map(money, values), money(sum(values))))
    return Answer(
        ("gender", "age_group", "q1_2011", "q2_2011", "q3_2011", "q4_2011", "total_2011"), rows
    )


def q5(frame: pd.DataFrame) -> Answer:
    groups = _sum_revenue(frame, ["product_category", "occupation"])
    top = _top_n(groups, ["product_category"], "occupation", "occupation_rank", 5)
    return Answer(
        ("product_category", "occupation_rank", "occupation", "total_sales"),
        [
            (r.product_category, r.occupation_rank, r.occupation, money(r.revenue_mills))
            for r in top.itertuples()
        ],
    )


def _latest_six_months(frame: pd.DataFrame) -> pd.DataFrame:
    months = frame[["year_no", "month_no"]].drop_duplicates().sort_values(["year_no", "month_no"])
    return months.tail(6)


def q6(frame: pd.DataFrame) -> Answer:
    recent = frame.merge(_latest_six_months(frame), on=["year_no", "month_no"])
    groups = _sum_revenue(
        recent, ["city_category", "marital_status", "year_no", "month_no", "month_name"]
    )
    return Answer(
        ("city_category", "marital_status", "year_no", "month_no", "month_name", "purchase_amount"),
        [
            (r.city_category, r.marital_status, r.year_no, r.month_no, r.month_name,
             money(r.revenue_mills))
            for r in groups.itertuples()
        ],
    )


def q7(frame: pd.DataFrame) -> Answer:
    rows = []
    for (stay, gender), group in frame.groupby(["stay_in_current_city_years", "gender"]):
        total = int(group["revenue_mills"].sum())
        transactions = len(group)
        carts = group["transaction_id"].nunique()
        rows.append(
            (
                stay,
                gender,
                transactions,
                carts,
                _rounded_division(Fraction(total, transactions * 1000), 3),
                _rounded_division(Fraction(total, carts * 1000), 3),
            )
        )
    return Answer(
        ("stay_in_current_city_years", "gender", "transactions", "carts",
         "avg_revenue_per_transaction", "avg_revenue_per_cart"),
        rows,
    )


def q8(frame: pd.DataFrame) -> Answer:
    groups = _sum_revenue(frame, ["product_category", "city_category"])
    groups["city_rank"] = (
        groups.groupby("product_category")["revenue_mills"]
        .rank(method="min", ascending=False)
        .astype("int64")
    )
    groups = groups.sort_values(["product_category", "city_rank", "city_category"])
    return Answer(
        ("product_category", "city_rank", "city_category", "revenue"),
        [
            (r.product_category, r.city_rank, r.city_category, money(r.revenue_mills))
            for r in groups.itertuples()
        ],
    )


def q9(frame: pd.DataFrame) -> Answer:
    monthly = _sum_revenue(frame, ["product_category", "year_no", "month_no", "month_name"])
    lagged = _with_previous(monthly, ["product_category"], ["year_no", "month_no"])
    rows = []
    for r in lagged[lagged["year_no"] == LOADED_YEAR].itertuples():
        previous = _optional_int(r.previous_mills)
        rows.append(
            (r.product_category, r.year_no, r.month_no, r.month_name, money(r.revenue_mills),
             _optional_money(previous), _percent_change(r.revenue_mills, previous))
        )
    return Answer(
        ("product_category", "year_no", "month_no", "month_name", "revenue", "previous_revenue",
         "growth_pct"),
        rows,
    )


def q10(frame: pd.DataFrame) -> Answer:
    year = frame[frame["year_no"] == LOADED_YEAR]
    rows = []
    for age_group, group in year.groupby("age_group"):
        weekday = int(_conditional_sum(group, "day_type", "Weekday", "revenue_mills").sum())
        weekend = int(_conditional_sum(group, "day_type", "Weekend", "revenue_mills").sum())
        rows.append(
            (age_group, money(weekday), money(weekend),
             _rounded_division(Fraction(100 * weekend, weekday + weekend), 2))
        )
    return Answer(("age_group", "weekday_revenue", "weekend_revenue", "weekend_share_pct"), rows)


def q11(frame: pd.DataFrame) -> Answer:
    year = frame[frame["year_no"] == LOADED_YEAR]
    groups = _sum_revenue(
        year, ["month_no", "month_name", "day_type", "product_id", "product_name"]
    )
    top = _top_n(groups, ["month_no", "day_type"], "product_id", "product_rank", 5)
    return Answer(
        ("month_no", "month_name", "day_type", "product_rank", "product_id", "product_name",
         "revenue"),
        [
            (r.month_no, r.month_name, r.day_type, r.product_rank, r.product_id, r.product_name,
             money(r.revenue_mills))
            for r in top.itertuples()
        ],
    )


def q12(frame: pd.DataFrame) -> Answer:
    quarterly = _sum_revenue(frame, ["store_name", "year_no", "quarter_no"])
    lagged = _with_previous(quarterly, ["store_name"], ["year_no", "quarter_no"])
    rows = []
    for r in lagged[lagged["year_no"] == LOADED_YEAR].itertuples():
        previous = _optional_int(r.previous_mills)
        rows.append(
            (r.store_name, r.year_no, r.quarter_no, money(r.revenue_mills),
             _optional_money(previous), _percent_change(r.revenue_mills, previous))
        )
    return Answer(
        ("store_name", "year_no", "quarter_no", "revenue", "previous_revenue", "growth_pct"),
        rows,
    )


def q13(frame: pd.DataFrame) -> Answer:
    groups = _sum_revenue(frame, ["store_name", "supplier_name", "product_name"])
    supplier_totals = groups.groupby(["store_name", "supplier_name"])["revenue_mills"].transform(
        "sum"
    )
    store_totals = groups.groupby("store_name")["revenue_mills"].transform("sum")
    rows = [
        (
            store,
            supplier,
            name,
            money(revenue),
            money(supplier_total),
            _half_up(Fraction(100 * int(supplier_total), int(store_total)), 4),
            _half_up(Fraction(100 * int(revenue), int(store_total)), 4),
        )
        for store, supplier, name, revenue, supplier_total, store_total in zip(
            groups["store_name"],
            groups["supplier_name"],
            groups["product_name"],
            groups["revenue_mills"],
            supplier_totals,
            store_totals,
            strict=True,
        )
    ]
    return Answer(
        ("store_name", "supplier_name", "product_name", "revenue", "supplier_revenue_in_store",
         "supplier_share_of_store_pct", "product_share_of_store_pct"),
        rows,
    )


def q14(frame: pd.DataFrame) -> Answer:
    pivot = frame.pivot_table(
        index=["product_id", "product_name"],
        columns="season",
        values="revenue_mills",
        aggfunc="sum",
        fill_value=0,
    ).sort_index()
    rows = []
    for (product_id, product_name), seasons in pivot.iterrows():
        values = [int(seasons[season]) for season in ("Spring", "Summer", "Fall", "Winter")]
        rows.append((product_id, product_name, *map(money, values), money(sum(values))))
    return Answer(
        ("product_id", "product_name", "spring_revenue", "summer_revenue", "fall_revenue",
         "winter_revenue", "total_revenue"),
        rows,
    )


def q15(frame: pd.DataFrame) -> Answer:
    monthly = _sum_revenue(
        frame, ["store_name", "supplier_name", "year_no", "month_no", "month_name"]
    )
    lagged = _with_previous(monthly, ["store_name", "supplier_name"], ["year_no", "month_no"])
    rows = []
    for r in lagged.itertuples():
        previous = _optional_int(r.previous_mills)
        rows.append(
            (r.store_name, r.supplier_name, r.year_no, r.month_no, r.month_name,
             money(r.revenue_mills), _optional_money(previous),
             _percent_change(r.revenue_mills, previous))
        )
    return Answer(
        ("store_name", "supplier_name", "year_no", "month_no", "month_name", "revenue",
         "previous_revenue", "volatility_pct"),
        rows,
    )


def q16(frame: pd.DataFrame) -> Answer:
    """Walks the carts one by one. Merging the rows with themselves would be the direct
    translation of the SQL, but it builds tens of millions of string rows. Each pair is
    numbered instead, with product ids in sorted order, so the order of pair numbers is
    also the order of the ids and serves as the tie break."""
    carts = frame[["transaction_id", "product_id"]].drop_duplicates()
    codes, product_ids = pd.factorize(carts["product_id"], sort=True)
    pair_numbers = []
    for positions in carts.groupby("transaction_id").indices.values():
        members = np.sort(codes[positions]).astype("int64")
        first, second = np.triu_indices(len(members), k=1)
        pair_numbers.append(members[first] * len(product_ids) + members[second])
    pairs, counts = np.unique(np.concatenate(pair_numbers), return_counts=True)
    top = np.lexsort((pairs, -counts))[:5]
    names = frame.drop_duplicates("product_id").set_index("product_id")["product_name"]
    rows = []
    for position in top:
        first_code, second_code = divmod(int(pairs[position]), len(product_ids))
        first_id, second_id = product_ids[first_code], product_ids[second_code]
        rows.append(
            (first_id, names[first_id], second_id, names[second_id], int(counts[position]))
        )
    return Answer(
        ("product_id_1", "product_name_1", "product_id_2", "product_name_2", "carts_together"),
        rows,
    )


def q17(frame: pd.DataFrame) -> Answer:
    """Built level by level with plain loops. The rollup is the very thing being checked,
    so this does not lean on any grouped-total feature of pandas."""
    rows: list[tuple] = []
    for year, by_year in frame.groupby("year_no"):
        for store, by_store in by_year.groupby("store_name"):
            for supplier, by_supplier in by_store.groupby("supplier_name"):
                products = _sum_revenue(by_supplier, ["product_id", "product_name"])
                for r in products.itertuples():
                    rows.append(
                        ("Product", year, store, supplier, r.product_id, r.product_name,
                         money(r.revenue_mills))
                    )
                rows.append(
                    ("Supplier subtotal", year, store, supplier, None, None,
                     money(by_supplier["revenue_mills"].sum()))
                )
            rows.append(
                ("Store subtotal", year, store, None, None, None,
                 money(by_store["revenue_mills"].sum()))
            )
        rows.append(
            ("Year total", year, None, None, None, None, money(by_year["revenue_mills"].sum()))
        )
    rows.append(
        ("Grand total", None, None, None, None, None, money(frame["revenue_mills"].sum()))
    )
    return Answer(
        ("row_level", "year_no", "store_name", "supplier_name", "product_id", "product_name",
         "revenue"),
        rows,
    )


def q18(frame: pd.DataFrame) -> Answer:
    year = frame[frame["year_no"] == LOADED_YEAR]
    summed = year.assign(
        h1_quantity=_conditional_sum(year, "half_year", "H1", "quantity"),
        h1_mills=_conditional_sum(year, "half_year", "H1", "revenue_mills"),
        h2_quantity=_conditional_sum(year, "half_year", "H2", "quantity"),
        h2_mills=_conditional_sum(year, "half_year", "H2", "revenue_mills"),
    )
    groups = summed.groupby(["product_id", "product_name"], as_index=False)[
        ["h1_quantity", "h1_mills", "h2_quantity", "h2_mills", "quantity", "revenue_mills"]
    ].sum()
    return Answer(
        ("product_id", "product_name", "h1_quantity", "h1_revenue", "h2_quantity", "h2_revenue",
         "year_quantity", "year_revenue"),
        [
            (r.product_id, r.product_name, r.h1_quantity, money(r.h1_mills), r.h2_quantity,
             money(r.h2_mills), r.quantity, money(r.revenue_mills))
            for r in groups.itertuples()
        ],
    )


def product_days(frame: pd.DataFrame) -> int:
    """How many distinct product and date combinations have a sale."""
    return len(frame[["product_id", "full_date"]].drop_duplicates())


def q19(frame: pd.DataFrame) -> Answer:
    daily = _sum_revenue(frame, ["product_id", "product_name", "full_date"])
    per_product = daily.groupby("product_id")["revenue_mills"]
    daily = daily.assign(
        product_total=per_product.transform("sum"), days_sold=per_product.transform("size")
    )
    # Cross-multiplied so that the test for "more than twice the average" is exact.
    spikes = daily[daily["revenue_mills"] * daily["days_sold"] > 2 * daily["product_total"]]
    rows = []
    for r in spikes.itertuples():
        exact_average = Fraction(int(r.product_total), int(r.days_sold) * 1000)
        rows.append(
            (
                r.product_id,
                r.product_name,
                r.full_date,
                money(r.revenue_mills),
                _rounded_division(exact_average, 3),
                r.days_sold,
                _rounded_division(
                    Fraction(int(r.revenue_mills) * int(r.days_sold), int(r.product_total)), 2
                ),
            )
        )
    return Answer(
        ("product_id", "product_name", "full_date", "daily_revenue", "avg_daily_revenue",
         "days_sold", "times_average"),
        rows,
    )


def q20(frame: pd.DataFrame) -> Answer:
    groups = _sum_revenue(frame, ["store_name", "year_no", "quarter_no"])
    return Answer(
        ("store_name", "year_no", "quarter_no", "total_sales"),
        [
            (r.store_name, r.year_no, r.quarter_no, money(r.revenue_mills))
            for r in groups.itertuples()
        ],
    )


_ANSWERS: dict[str, Callable[[pd.DataFrame], Answer]] = {
    "Q1": q1, "Q2": q2, "Q3": q3, "Q4": q4, "Q5": q5, "Q6": q6, "Q7": q7, "Q8": q8, "Q9": q9,
    "Q10": q10, "Q11": q11, "Q12": q12, "Q13": q13, "Q14": q14, "Q15": q15, "Q16": q16,
    "Q17": q17, "Q18": q18, "Q19": q19, "Q20": q20,
}

QUESTION_IDS: Sequence[str] = tuple(_ANSWERS)


def expected_answer(question: str, frame: pd.DataFrame) -> Answer:
    """The expected columns and rows for one question."""
    return _ANSWERS[question](frame)
