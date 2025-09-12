# models/instrument_data.py
from dataclasses import dataclass, field
from datetime import date
from typing import Literal, Optional, Dict, Any

DataType = Literal[
    "clean_price", "dirty_price", "yield", "z_spread", "ois_oas",
    "accrued_interest", "modified_duration", "amount_outstanding", "rpi"
]
Resolution = Literal[
    "daily", "weekly", "monthly", "irregular",
    "daily_close", "daily_open"
]

@dataclass(slots=True, frozen=True, kw_only=True)
class InstrumentData:
    instrument_id: str              # FK to instruments.isin
    data_date: date                 # valuation/as-of date
    data_type: DataType
    value: float
    source: str                     # "Tradeweb","DMO","Model",...
    resolution: Resolution = "daily"

    unit: Optional[str] = None      # "per_100","percent","index","bps"
    attrs: Dict[str, Any] = field(default_factory=dict)
    # Will be stored as JSON in DB, but exposed as dict in Python