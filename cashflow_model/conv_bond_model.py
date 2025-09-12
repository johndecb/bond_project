from __future__ import annotations
from dataclasses import dataclass
from math import isfinite
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional, Literal
from dateutil.relativedelta import relativedelta
from models.cashflows import CashflowRow

def _coerce_date(x) -> date:
    """
    Accepts: datetime.date, datetime.datetime, pandas.Timestamp (without importing pandas),
            or a CashflowRow (uses .adjusted_date).
    Returns: datetime.date
    """
    # If it's a CashflowRow, use the adjusted payment date
    x = getattr(x, "adjusted_date", x)

    # Already a date
    if isinstance(x, date):
        return x

    # Datetime-like (includes pandas.Timestamp which has .date())
    if hasattr(x, "date"):
        return x.date()

    raise TypeError(f"Expected a date-like value, got {type(x)!r} with value {x!r}")

class CashflowModel:
    def __init__(
        self,
        issue_date: date,
        maturity_date: date,
        coupon_rate: float,
        frequency: int,
        *,
        rate_unit: Literal["auto","percent","decimal"] = "auto",
        notional: float = 100.0,
        calendar: Optional[object] = None,
        convention: str = "mf",  # modified following
        first_coupon_length: Optional[str] = None,  # "Short First" / "Long First" / None (informational)
        first_coupon_date: Optional[date] = None,   # if provided, anchors the first coupon end
        last_coupon_date: Optional[date] = None,    # if provided, anchors the last coupon end (before maturity)
    ):
        # normalize coupon_rate to decimal
        if rate_unit == "percent":
            cr = coupon_rate / 100.0
        elif rate_unit == "decimal":
            cr = coupon_rate
        else:  # "auto" (gilts-friendly)
            # Treat anything in a realistic gilt range as a PERCENT input.
            # i.e., 0.125 -> 0.125%, 0.25 -> 0.25%, 12.5 -> 12.5%
            # Only treat as decimal if the number is implausibly large for a coupon.
            cr = coupon_rate / 100.0 if 0.0 <= coupon_rate <= 20.0 else coupon_rate

        if cr < 0:
            raise ValueError("coupon_rate cannot be negative.")
        
        self.coupon_rate = cr
        self.issue_date = issue_date
        self.maturity_date = maturity_date
        self.frequency = frequency
        self.notional = notional
        self.calendar = calendar
        self.convention = convention
        self.first_coupon_length = first_coupon_length
        self.first_coupon_date = first_coupon_date
        self.last_coupon_date = last_coupon_date

        if frequency not in (1, 2, 4, 12):
            raise ValueError("frequency must be 1, 2, 4, or 12 coupons per year.")

    # ---- helpers -------------------------------------------------------------

    def _adjust(self, dt: date) -> date:
        if self.calendar:
            return self.calendar.adjust(dt, self.convention)
        return dt

    @staticmethod
    def _days(a: date, b: date) -> int:
        return (b - a).days

    def _regular_period_days(self, quasi_end: date, months: int) -> int:
        """Nominal regular period length in days (used as denominator for stubs)."""
        nominal_start = quasi_end - relativedelta(months=months)
        return self._days(nominal_start, quasi_end)

    def _accrual_factor(self, start: date, end: date, quasi_end: date, months: int) -> float:
        """Regular periods → 1/frequency. Stub → ACT/ACT-like prorating."""
        # Regular if start is exactly nominal_start (quasi_end - months)
        nominal_start = quasi_end - relativedelta(months=months)
        if start == nominal_start: return 1.0 / self.frequency
        
        actual = max(self._days(start, end), 0)
        denom = max(self._regular_period_days(quasi_end, months), 1)
        return (actual / denom) / self.frequency

    def _coupon_amount(self, accrual_factor: float) -> float:
        # coupon_rate is already decimal after normalization in __init__
        return self.notional * self.coupon_rate * accrual_factor

    # ---- settlement helpers --------------------------------------------------

    def _months_per_period(self) -> int:
        return 12 // self.frequency
    
    def _find_period_row(self, schedule, settlement: date):
        """
        Returns (row, idx) for the coupon period containing `settlement`,
        where period_start <= settlement < period_end. If settlement is before
        first period_start, returns (None, None).
        """
        for idx, row in enumerate(schedule):
            if row.period_start <= settlement < row.period_end:
                return row, idx
        return None, None

    def _ex_div_date(self, pay_dt, ex_div_business_days: int) -> date:
        pay_dt = _coerce_date(pay_dt)
        if ex_div_business_days <= 0:
            return pay_dt

        cal = getattr(self, "calendar", None)
        if cal and hasattr(cal, "shift_business_days"):
            return cal.shift_business_days(pay_dt, -ex_div_business_days)
        if cal and hasattr(cal, "workday"):
            return cal.workday(pay_dt, -ex_div_business_days)

        return pay_dt - timedelta(days=ex_div_business_days)

    # ---- price/yield core (street convention) --------------------------------

    def _pv_dirty_from_yield(self, y: float, settlement: date, ex_div_business_days: int) -> float:
        """
        Dirty price per 100 notional from street-convention yield y.
        Uses nominal compounding at `frequency`; fractional power (n - s),
        where s is the fraction of the current coupon period accrued.
        Ex-div: first upcoming coupon is excluded if settlement >= ex-div date.
        """
        schedule = self.generate_cashflow_schedule()

        # Any future cash flows?
        future_rows = [r for r in schedule if r.period_end > settlement]
        if not future_rows:
            return 0.0  # nothing left to PV

        # Find the period containing settlement (to compute s)
        current_row = None
        for r in schedule:
            if r.period_start <= settlement < r.period_end:
                current_row = r
                break

        # Elapsed fraction of coupon period, s \in [0,1] (ICMA stub logic)
        months = 12 // self.frequency
        if current_row is None:
            # If settlement is before first period_start (rare) or weird edge case,
            # take s = 0 so first remaining CF gets full period exponent.
            s = 0.0
        else:
            af = self._accrual_factor(current_row.period_start, settlement,
                                    current_row.quasi_date, months)  # in "years of coupon"
            s = max(0.0, min(af * self.frequency, 1.0))

        # Ex-div gate: zero ONLY the first upcoming coupon if within ex-div window
        first = future_rows[0]
        if settlement >= self._ex_div_date(first.adjusted_date, ex_div_business_days) and settlement < first.period_end:
            first = CashflowRow(**{**first.__dict__, "coupon_amount": 0.0})
            future_rows = [first] + future_rows[1:]

        # Nominal compounding at coupon frequency
        f = self.frequency
        one_plus = 1.0 + (y / f)
        if one_plus <= 0.0:
            raise ValueError("Yield too low for nominal compounding at this frequency.")

        pv = 0.0
        for n, r in enumerate(future_rows, start=1):
            power = n - s          # fractional periods remaining
            df = one_plus ** (-power)
            pv += (r.coupon_amount + r.principal) * df

        return pv

    def dirty_price_from_yield(self, y: float, settlement: date, ex_div_business_days: int = 7) -> float:
        return self._pv_dirty_from_yield(y, settlement, ex_div_business_days)

    def clean_price_from_yield(self, y: float, settlement: date, ex_div_business_days: int = 7) -> float:
        dirty = self.dirty_price_from_yield(y, settlement, ex_div_business_days)
        ai = self.accrued_interest(settlement)
        return dirty - ai

    def accrued_interest(self, settlement: date, ex_div_business_days: int = 7) -> float:
        """
        Accrued interest per 100 notional at 'settlement'.
        If 'settlement' is on/after the ex-div date for the *next* coupon,
        accrued turns negative and accrues toward the payment date.
        """
        months = 12 // self.frequency
        schedule = self.generate_cashflow_schedule()

        # Find the coupon period that contains settlement (strictly before period_end)
        cur_row = None
        for r in schedule:
            if r.period_start <= settlement < r.period_end:
                cur_row = r
                break

        # If we are on/after maturity (or not in any period), AI = 0
        if cur_row is None or settlement >= self.maturity_date:
            return 0.0

        # Ex-div date for the upcoming payment
        exd = self._ex_div_date(cur_row.adjusted_date, ex_div_business_days)

        if settlement < exd:
            # ---- Normal accrued: from period_start up to settlement ----
            af = self._accrual_factor(cur_row.period_start, settlement,
                                    cur_row.quasi_date, months)
            return self._coupon_amount(af)

        # ---- Ex-div window: negative accrued from settlement to payment ----
        # Use the same ICMA-like prorating you use for stubs/regular periods.
        af_rem = self._accrual_factor(settlement, cur_row.period_end,
                                    cur_row.quasi_date, months)
        return -self._coupon_amount(af_rem)


    # ---- root finders for yield ----------------------------------------------

    def yield_from_clean_price(
        self,
        clean_price: float,
        settlement: date,
        ex_div_business_days: int = 7,
        y0: float = None,
        tol: float = 1e-9,
        max_iter: int = 8,
        bracket: tuple = None,
    ) -> float:
        target_dirty = clean_price + self.accrued_interest(settlement, ex_div_business_days)

        def f(y):
            return self._pv_dirty_from_yield(y, settlement, ex_div_business_days) - target_dirty

        fqy = self.frequency
        min_y = -0.99 * fqy + 1e-9  # keep 1 + y/f > 0

        # 1) If user supplied a bracket, honour it (and expand a bit if needed)
        if bracket is not None:
            a, b = max(min_y, bracket[0]), max(min_y + 1e-9, bracket[1])
            fa, fb = f(a), f(b)
            expand = 0
            while fa * fb > 0 and expand < 12:
                # expand symmetrically
                width = (b - a)
                a = max(min_y, a - width)
                b = b + width
                fa, fb = f(a), f(b)
                expand += 1
            if fa == 0: return float(a)
            if fb == 0: return float(b)
            if fa * fb < 0:
                lo, hi, flo, fhi = a, b, fa, fb
                # bisection
                for _ in range(max_iter):
                    mid = 0.5 * (lo + hi)
                    fm = f(mid)
                    if abs(fm) < tol: return float(mid)
                    if flo * fm <= 0:
                        hi, fhi = mid, fm
                    else:
                        lo, flo = mid, fm
                return float(0.5 * (lo + hi))
            # fall through to grid scan if expansion didn't find a sign change

        # 2) Build anchor grid (include y0 and coupon as hints)
        anchors = [min_y + 1e-9, -0.75*fqy, -0.5*fqy, -0.25*fqy, -0.10*fqy, -0.05,
                0.0, 0.01, 0.02, 0.05, 0.10, 0.20, 0.50, 1.0, 2.0, 5.0, 10.0]
        # Use y0 if given
        if y0 is not None:
            anchors += [y0 - 0.05, y0 + 0.05]
        # Use coupon rate (rough near-par hint)
        anchors += [max(min_y, self.coupon_rate - 0.05), self.coupon_rate + 0.05]

        # Deduplicate and sort
        anchors = sorted({a for a in anchors if a > min_y})

        # Evaluate and find first sign change
        last_y = None
        last_f = None
        for y in anchors:
            try:
                fy = f(y)
            except Exception:
                continue
            if fy == 0:
                return float(y)
            if last_y is not None and last_f is not None and (fy * last_f) < 0:
                lo, hi = min(last_y, y), max(last_y, y)
                flo, fhi = (last_f, fy) if lo == last_y else (fy, last_f)
                # bisection
                for _ in range(max_iter):
                    mid = 0.5 * (lo + hi)
                    fm = f(mid)
                    if abs(fm) < tol: return float(mid)
                    if flo * fm <= 0:
                        hi, fhi = mid, fm
                    else:
                        lo, flo = mid, fm
                return float(0.5 * (lo + hi))
            last_y, last_f = y, fy

        raise RuntimeError("Failed to bracket the yield. Check settlement vs maturity, coupon normalisation, and ex-div.")


    # ---- schedule builder ----------------------------------------------------

    def generate_cashflow_schedule(self) -> List[CashflowRow]:
        months = self._months_per_period()

        # 1) Always build from MATURITY backwards
        quasi_dates: List[date] = []
        cur = self.maturity_date
        while cur > self.issue_date:
            quasi_dates.append(cur)
            cur = cur - relativedelta(months=months)

        # Always include the issue date
        quasi_dates.append(self.issue_date)

        # If the user gave an explicit last regular coupon date (before maturity), include it
        if self.last_coupon_date:
            if not (self.issue_date < self.last_coupon_date < self.maturity_date):
                raise ValueError("last_coupon_date must be strictly between issue_date and maturity_date.")
            quasi_dates.append(self.last_coupon_date)

        # 2) Compute effective first cash date (unchanged logic)
        first_end_eff: Optional[date] = None
        if (self.first_coupon_length or "").strip() == "Long First":
            if self.first_coupon_date:
                first_end_eff = self.first_coupon_date + relativedelta(months=months)
            else:
                first_nominal = min(d for d in quasi_dates if d > self.issue_date)
                first_end_eff = first_nominal + relativedelta(months=months)
        elif self.first_coupon_date:
            first_end_eff = self.first_coupon_date

        if first_end_eff:
            # Ensure it’s between issue and maturity
            if not (self.issue_date < first_end_eff <= self.maturity_date):
                raise ValueError("first_coupon_date / effective first cash date must be within (issue, maturity].")
            quasi_dates.append(first_end_eff)

        # 3) Sort + unique
        quasi_dates = sorted(set(quasi_dates))

        # 4) If we’ve created a long-first effective endpoint, drop in-between boundaries
        if first_end_eff:
            quasi_dates = [d for d in quasi_dates if not (self.issue_date < d < first_end_eff)]
            quasi_dates = sorted(quasi_dates)

        assert len(quasi_dates) >= 2, "Schedule needs at least a start and an end date."
        assert quasi_dates[-1] == self.maturity_date, "Last quasi-date must be maturity_date."

        rows: List[CashflowRow] = []
        for i in range(1, len(quasi_dates)):
            period_start = quasi_dates[i - 1]
            period_end   = quasi_dates[i]
            if period_end <= period_start:
                continue

            quasi_end    = period_end
            adjusted_end = self._adjust(quasi_end)

            # Stub classification
            is_first = (i == 1)
            is_last_before_maturity = (period_end == (self.last_coupon_date or self.maturity_date))
            is_stub = False
            note = None

            nominal_start = quasi_end - relativedelta(months=months)

            if is_first and period_start != nominal_start:
                is_stub = True
                if self.first_coupon_length in ("Short First", "Long First"):
                    note = self.first_coupon_length
                else:
                    note = "Short First" if period_start > nominal_start else "Long First"

            if self.last_coupon_date and is_last_before_maturity and period_start != nominal_start:
                is_stub = True
                note = (note + " + Last Stub") if note else "Last Stub"

            accrual = self._accrual_factor(period_start, period_end, quasi_end, months)
            coupon  = self._coupon_amount(accrual)

            # Principal only at maturity
            principal = self.notional if period_end == self.maturity_date else 0.0

            rows.append(
                CashflowRow(
                    period_start=period_start,
                    period_end=period_end,
                    quasi_date=quasi_end,
                    adjusted_date=adjusted_end,
                    is_stub=is_stub,
                    accrual_factor=accrual,
                    coupon_amount=round(coupon, 10),
                    principal=principal,
                    note=note,
                )
            )

        return rows
