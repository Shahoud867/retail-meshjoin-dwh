"""The fixed text and number rules of PDF sections 5.4 and 8.

Pure functions and standard library only, because consolidation calls them for
every transaction at run time, long after pandas has any reason to be loaded.
"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime
from typing import Any

from retail_dw.meshjoin.model import SALES_MODE_ONLINE, SALES_MODE_PERIODIC

__all__ = [
    "STORE_LIST",
    "canonical_id",
    "md5_number",
    "sales_mode_for",
    "stable",
    "store_id_for",
    "time_parts",
]

# PDF section 5.4.2: store_id -> (store_name, city). Held in memory because the
# store is attached outside MESHJOIN.
STORE_LIST: dict[int, tuple[str, str]] = {
    1: ("Online", "Web"),
    2: ("London Bridge", "London"),
    3: ("Manchester Arndale", "Manchester"),
    4: ("Birmingham Bullring", "Birmingham"),
    5: ("Edinburgh Princes", "Edinburgh"),
}

_ONLINE_STORE_ID = 1
_FIRST_PHYSICAL_STORE_ID = 2
_PHYSICAL_STORE_COUNT = 4
_ONLINE_REMAINDERS = 7  # Remainders 0 to 6 of 10 are online, 7 to 9 are physical.

# Written out because strftime("%A") follows the machine's locale, and a time
# dimension must not depend on where the program happens to run.
_MONTH_NAMES = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
_DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
_SEASONS_BY_MONTH = (
    "Winter",
    "Winter",
    "Spring",
    "Spring",
    "Spring",
    "Summer",
    "Summer",
    "Summer",
    "Fall",
    "Fall",
    "Fall",
    "Winter",
)


def canonical_id(value: Any) -> str | None:
    """Return an id as the text the whole project uses, or None if it is missing.

    A numeric cell can reach us as a float, and str(17850.0) would create a customer
    called "17850.0" that matches nothing. Integer-like floats therefore go through
    int first. Text is kept as it is, because ids are compared exactly.
    """
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
    return str(value)


def md5_number(text: str) -> int:
    """Return the first 8 hexadecimal characters of the text's MD5 as a number."""
    return int(hashlib.md5(text.encode("utf-8")).hexdigest()[:8], 16)


def stable(text: str, n: int) -> int:
    """Return the PDF's stable(text, n): the same text always gives the same index."""
    return md5_number(text) % n


def sales_mode_for(transaction_id: str) -> str:
    """Decide the sales mode from the invoice number, so a whole cart gets one mode."""
    if md5_number(transaction_id) % 10 < _ONLINE_REMAINDERS:
        return SALES_MODE_ONLINE
    return SALES_MODE_PERIODIC


def store_id_for(transaction_id: str, sales_mode: str) -> int:
    """Return the store of a cart: the website if online, else one of the four shops."""
    if sales_mode == SALES_MODE_ONLINE:
        return _ONLINE_STORE_ID
    return _FIRST_PHYSICAL_STORE_ID + md5_number(transaction_id) % _PHYSICAL_STORE_COUNT


def time_parts(dt: datetime) -> dict[str, Any]:
    """Return one dim_time row, keyed and ordered by the dim_time column names.

    The key has minute resolution, so full_datetime is cut to the minute as well.
    Otherwise two values in one minute would share a key but not a datetime. It is
    also rebuilt as a plain datetime, because a pandas Timestamp may be passed in.
    """
    month = dt.month
    weekday = dt.isoweekday()
    return {
        "time_key": (
            dt.year * 100_000_000
            + month * 1_000_000
            + dt.day * 10_000
            + dt.hour * 100
            + dt.minute
        ),
        "full_datetime": datetime(dt.year, month, dt.day, dt.hour, dt.minute),
        "full_date": dt.date(),
        "hour_of_day": dt.hour,
        "day_of_month": dt.day,
        "month_no": month,
        "month_name": _MONTH_NAMES[month - 1],
        "quarter_no": (month - 1) // 3 + 1,
        "year_no": dt.year,
        "day_of_week": weekday,
        "day_name": _DAY_NAMES[weekday - 1],
        "day_type": "Weekend" if weekday >= 6 else "Weekday",
        "season": _SEASONS_BY_MONTH[month - 1],
        "half_year": "H1" if month <= 6 else "H2",
    }
