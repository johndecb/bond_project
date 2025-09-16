from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass(slots=True, frozen=True)
class CashflowRow:
    """
    Canonical cashflow row used across pricing, accrual, and exports.
    - principal: 0.0 except at maturity (redemption)
    - note: optional free-text (e.g., 'long first', 'ex-div', etc.)
    """
    period_start: date
    period_end: date
    quasi_date: date
    adjusted_date: date
    is_stub: bool
    accrual_factor: float
    coupon_amount: float
    principal: float = 0.0
    note: Optional[str] = None