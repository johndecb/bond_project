from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass(slots=True, frozen=True, kw_only=True)
class Instrument:
    isin: str
    short_code: str
    name: str
    instrument_type: str
    issuer: str
    country: str
    currency: str
    maturity_date: Optional[date]       # now Optional
    first_issue_date: Optional[date]    # now Optional

    coupon_rate: Optional[float] = None
    first_coupon_length: Optional[str] = None
    is_green: bool = False
    is_linker: bool = False
    index_lag: Optional[int] = None
    rpi_base: Optional[float] = None
    tenor: Optional[str] = None
    reference_index: Optional[str] = None
    day_count_fraction: Optional[str] = None

    def __post_init__(self):
        it = (self.instrument_type or "").strip().lower()

        # Bonds must have both dates
        if self.first_issue_date is not None and self.maturity_date is not None:
            if self.first_issue_date > self.maturity_date:
                raise ValueError("first_issue_date must be <= maturity_date")

        # 2) Bonds must have both dates present
        if it == "bond":
            if self.first_issue_date is None or self.maturity_date is None:
                raise ValueError("Bonds require first_issue_date and maturity_date")