import os
from datetime import date, datetime, timedelta

def get_settlement_date(fallback: date | None = None) -> date:
    """
    Return settlement date:
    - If `fallback` is provided, return it.
    - Else try env var SETTLEMENT_DATE.
    - Else default to today + 1 (T+1).
    """
    if fallback:
        return fallback

    env_date = os.getenv("SETTLEMENT_DATE")
    if env_date:
        return datetime.strptime(env_date, "%Y-%m-%d").date()

    return date.today() + timedelta(days=1)

