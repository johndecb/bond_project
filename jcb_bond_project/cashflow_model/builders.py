from datetime import date, datetime
from typing import List, Dict, Optional, Any
import pandas as pd
import numpy as np

from jcb_bond_project.models.instrument import Instrument
from jcb_bond_project.cashflow_model.conv_bond_model import CashflowModel, CashflowRow
from dateutil.relativedelta import relativedelta


def cashflows_from_instrument(
    inst: Instrument,
    *,
    calendar=None,
    frequency: int = 2,
    notional: float = 100.0,
    convention: str = "mf",
    first_coupon_date: Optional[date] = None,
) -> List[CashflowRow]:
    if (inst.instrument_type or "").strip().lower() != "bond":
        raise ValueError(f"Expected a bond, got {inst.instrument_type!r}")
    if inst.first_issue_date is None or inst.maturity_date is None or inst.coupon_rate is None:
        raise ValueError(f"Missing dates/coupon for {inst.isin}")

    model = CashflowModel(
        issue_date=inst.first_issue_date,
        maturity_date=inst.maturity_date,
        coupon_rate=float(inst.coupon_rate),
        frequency=frequency,
        notional=notional,
        calendar=calendar,
        convention=convention,
        first_coupon_length=inst.first_coupon_length,
        first_coupon_date=first_coupon_date,
    )
    return model.generate_cashflow_schedule()



