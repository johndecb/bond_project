# database/insert.py

from __future__ import annotations
import sqlite3
import json
from dataclasses import asdict
from datetime import date, datetime
from typing import Dict, Any, Iterable, Optional, Tuple

from jcb_bond_project.models.instrument import Instrument
from jcb_bond_project.models.instrument_data import InstrumentData


# ------------ helpers ------------

def _iso(x) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date().isoformat()
    if isinstance(x, date):
        return x.isoformat()
    return str(x)

def _instrument_to_params(inst: Instrument) -> Dict[str, Any]:
    """
    Map Instrument -> DB row dict (column names must match 'instruments' table).
    Converts dates to ISO strings; booleans to ints for SQLite.
    """
    d = asdict(inst)
    for k, v in list(d.items()):
        if isinstance(v, (date, datetime)):
            d[k] = _iso(v)
        elif isinstance(v, bool):
            d[k] = int(v)
    # ensure PK field name exists and is called 'isin'
    if "isin" not in d:
        raise ValueError("Instrument is missing 'isin'")
    return d


# ------------ instruments ------------

def save_instrument(conn: sqlite3.Connection, instrument: Instrument) -> str:
    """
    Upsert an instrument by its natural PK 'isin'.
    Returns 'updated' if it existed, else 'inserted'.
    """
    params = _instrument_to_params(instrument)

    # Detect insert vs update (optional)
    existed = conn.execute(
        "SELECT 1 FROM instruments WHERE isin = ? LIMIT 1", (instrument.isin,)
    ).fetchone() is not None

    cols = ", ".join(params.keys())
    named = ", ".join(f":{k}" for k in params.keys())
    updates = ", ".join(f"{k}=excluded.{k}" for k in params.keys() if k != "isin")

    sql = f"""
        INSERT INTO instruments ({cols})
        VALUES ({named})
        ON CONFLICT(isin) DO UPDATE SET
            {updates}
    """
    conn.execute(sql, params)
    return "updated" if existed else "inserted"


def update_instrument_field_by_id(
    conn: sqlite3.Connection,
    isin: str,
    field_name: str,
    new_value: Any,
) -> str:
    """
    Update a single allowed column in instruments by ISIN.
    """
    allowed_fields = {
        "short_code", "name", "instrument_type", "issuer", "country", "currency",
        "maturity_date", "first_issue_date", "coupon_rate", "first_coupon_length",
        "is_green", "is_linker", "index_lag", "rpi_base", "tenor",
        "reference_index", "day_count_fraction",
    }
    if field_name not in allowed_fields:
        raise ValueError(f"Field '{field_name}' is not allowed to be updated.")

    # Normalise dates/bools
    if isinstance(new_value, (date, datetime)):
        new_value = _iso(new_value)
    if isinstance(new_value, bool):
        new_value = int(new_value)

    sql = f"UPDATE instruments SET {field_name} = ? WHERE isin = ?"
    cur = conn.execute(sql, (new_value, isin))
    return "updated" if cur.rowcount == 1 else "not_found"


# ------------ instrument_data ------------

def insert_instrument_data(conn: sqlite3.Connection, instrument_data: InstrumentData) -> str:
    """
    Insert (ignore on duplicate) one InstrumentData row.
    Schema PK: (instrument_id, data_date, data_type, source, resolution)
    """
    fields = {
        "instrument_id": instrument_data.instrument_id,
        "data_date": _iso(instrument_data.data_date),
        "data_type": instrument_data.data_type,
        "value": float(instrument_data.value) if instrument_data.value is not None else None,
        "source": instrument_data.source,
        "resolution": instrument_data.resolution,
        "unit": instrument_data.unit,
        "attrs": json.dumps(instrument_data.attrs or {}, separators=(",", ":"), ensure_ascii=False),
    }

    cols = ", ".join(fields.keys())
    placeholders = ", ".join("?" for _ in fields)

    sql = f"""
        INSERT OR IGNORE INTO instrument_data ({cols})
        VALUES ({placeholders})
    """
    cur = conn.execute(sql, tuple(fields.values()))
    return "inserted" if cur.rowcount == 1 else "skipped"


# (optional) efficient bulk insert if you have many rows
def bulk_insert_instrument_data(
    conn: sqlite3.Connection,
    rows: Iterable[InstrumentData],
) -> Tuple[int, int]:
    """
    Insert many InstrumentData rows. Returns (inserted_count, skipped_count).
    """
    prepared = []
    for r in rows:
        prepared.append((
            r.instrument_id,
            _iso(r.data_date),
            r.data_type,
            float(r.value) if r.value is not None else None,
            r.source,
            r.resolution,
            r.unit,
            json.dumps(r.attrs or {}, separators=(",", ":"), ensure_ascii=False),
        ))
    sql = """
        INSERT OR IGNORE INTO instrument_data
        (instrument_id, data_date, data_type, value, source, resolution, unit, attrs)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.executemany(sql, prepared)
    # sqlite3 doesn't give rowcount for executemany reliably; recompute:
    inserted = conn.execute("SELECT changes();").fetchone()[0]
    skipped = len(prepared) - inserted
    return inserted, skipped


# ------------ alternate identifiers ------------

def insert_instrument_identifier(
    conn: sqlite3.Connection,
    isin: str,
    alt_id: str,
    source: str,
) -> str:
    """
    Insert link to an alternate identifier.
    Table: instrument_identifiers (plural)
    PK: (instrument_id, identifier_string, identifier_source)
    """
    sql = """
        INSERT OR IGNORE INTO instrument_identifiers
        (instrument_id, identifier_string, identifier_source)
        VALUES (?, ?, ?)
    """
    cur = conn.execute(sql, (isin, alt_id, source))
    return "inserted" if cur.rowcount == 1 else "skipped"


# ------------ calendar holidays ------------

def insert_calendar_holidays(
    conn: sqlite3.Connection,
    calendar_name: str,
    holidays_with_desc: Iterable[Tuple[date | str, Optional[str]]],
) -> int:
    """
    Bulk insert/merge holidays for a calendar.
    Returns number of newly inserted rows.
    """
    sql = """
        INSERT OR IGNORE INTO calendar_holidays
        (calendar_name, holiday_date, description)
        VALUES (?, ?, ?)
    """
    inserted = 0
    for holiday_date, description in holidays_with_desc:
        cur = conn.execute(sql, (calendar_name, _iso(holiday_date), description))
        inserted += cur.rowcount
    return inserted