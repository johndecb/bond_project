# utils/calendar.py
from __future__ import annotations
from datetime import date as _date, datetime, timedelta
from typing import Iterable, Optional, Set, Literal, Any
from functools import lru_cache

RollConv = Literal["f", "p", "mf"]  # following, preceding, modified following

def _ensure_date(d: Any) -> _date:
    """
    Coerce datetime / pandas.Timestamp to date.
    Refuse dataclasses like CashflowRow to avoid accidental timedelta ops on rows.
    """
    # Accept datetime-like with .date()
    if hasattr(d, "date") and not isinstance(d, _date):
        d = d.date()
    if not isinstance(d, _date):
        raise TypeError(f"Expected datetime.date-like, got {type(d)!r}")
    return d

# ---- keep your existing function, now with guards ----
def adjust_to_business_day(d, holidays: Set[_date], convention: RollConv = "f") -> _date:
    d = _ensure_date(d)

    @lru_cache(maxsize=256_000)
    def is_business_day(x: _date) -> bool:
        return x.weekday() < 5 and x not in holidays

    original_month = d.month

    if convention == "f":
        while not is_business_day(d):
            d += timedelta(days=1)
        return d

    if convention == "p":
        while not is_business_day(d):
            d -= timedelta(days=1)
        return d

    if convention == "mf":
        adjusted = d
        while not is_business_day(adjusted):
            adjusted += timedelta(days=1)
        if adjusted.month != original_month:
            adjusted = d
            while not is_business_day(adjusted):
                adjusted -= timedelta(days=1)
        return adjusted

    raise ValueError(f"Unsupported convention: {convention!r}")

# ---- lightweight class wrapper so models can use calendar.adjust(...) ----
class BusinessDayCalendar:
    def __init__(self, holidays: Iterable[_date], weekend: Optional[Set[int]] = None):
        """
        holidays: iterable of datetime.date
        weekend: set of weekday ints treated as weekend (default {5,6} => Sat/Sun)
        """
        self.holidays: Set[_date] = { _ensure_date(h) for h in holidays }
        self.weekend: Set[int] = {5, 6} if weekend is None else set(weekend)

    @lru_cache(maxsize=256_000)
    def is_business_day(self, d: _date) -> bool:
        d = _ensure_date(d)
        return (d.weekday() not in self.weekend) and (d not in self.holidays)

    def adjust(self, d: _date, convention: RollConv = "f") -> _date:
        return adjust_to_business_day(d, self.holidays, convention)

    def workday(self, start: _date, days: int) -> _date:
        """
        Excel-like WORKDAY: move N business days from 'start'.
        - If days == 0: return 'start' if it's a biz day, otherwise the next biz day.
        - Start date itself is not counted when |days| > 0.
        """
        start = _ensure_date(start)
        if days == 0:
            return start if self.is_business_day(start) else self.next_business_day(start)

        step = 1 if days > 0 else -1
        remaining = abs(days)
        d = start
        while remaining:
            d += timedelta(days=step)
            if self.is_business_day(d):
                remaining -= 1
        return d

    def next_business_day(self, d: _date) -> _date:
        d = _ensure_date(d) + timedelta(days=1)
        while not self.is_business_day(d):
            d += timedelta(days=1)
        return d

    def previous_business_day(self, d: _date) -> _date:
        d = _ensure_date(d) - timedelta(days=1)
        while not self.is_business_day(d):
            d -= timedelta(days=1)
        return d

    def shift_business_days(self, d: _date, n: int) -> _date:
        """Alias of workday(d, n)."""
        return self.workday(d, n)

    def business_days_between(
        self,
        a: _date,
        b: _date,
        *,
        inclusive: bool = False,
        signed: bool = True,
    ) -> int:
        """
        Count business days between a and b.
        - inclusive=False: counts in (a, b)  (start excluded, end excluded)
        - inclusive=True:  counts in [a, b] (start included, end included)
        - signed: if True, result is negative when a > b; if False, absolute value is returned.
        """
        a = _ensure_date(a)
        b = _ensure_date(b)

        if a == b:
            count = 1 if (inclusive and self.is_business_day(a)) else 0
            return count

        sign = 1
        start, end = a, b
        if a > b:
            start, end = b, a
            sign = -1

        cur = start
        count = 0
        while cur <= end:
            if (inclusive or (start < cur < end)) and self.is_business_day(cur):
                count += 1
            cur += timedelta(days=1)

        return count * (sign if signed else 1)

