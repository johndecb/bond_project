# database/insert.py

import json
from dataclasses import asdict
from datetime import date, datetime
from typing import Any, Dict, Iterable, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

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
    d = asdict(inst)
    for k, v in list(d.items()):
        if isinstance(v, (date, datetime)):
            d[k] = _iso(v)
    return d


# ------------ instruments ------------

def save_instrument(conn, instrument: Instrument) -> str:
    """Upsert an instrument by ISIN."""
    params = _instrument_to_params(instrument)
    cols = ", ".join(params.keys())
    placeholders = ", ".join([f"%({k})s" for k in params.keys()])
    updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in params.keys() if k != "isin"])

    sql = f"""
        INSERT INTO instruments_instrument ({cols})
        VALUES ({placeholders})
        ON CONFLICT (isin) DO UPDATE SET {updates}
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
    return "inserted_or_updated"


# ------------ data type normalisation ------------

def normalise_data_type(conn, source: str, raw_type: str) -> Tuple[str, Optional[str]]:
    """Look up canonical data_type/unit given source + raw_type."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT canonical_data_type, default_unit
            FROM instruments_datatypemap
            WHERE (LOWER(source) = LOWER(%s) OR source = '*')
              AND LOWER(raw_data_type) = LOWER(%s)
            ORDER BY CASE WHEN source = '*' THEN 2 ELSE 1 END
            LIMIT 1
        """, (source, raw_type))
        row = cur.fetchone()
        if row:
            return row[0], row[1]
        return raw_type, None


# ------------ instrument_data ------------

def insert_instrument_data(conn, instrument_data: InstrumentData) -> str:
    """Insert one InstrumentData row, auto-normalising data_type."""
    canonical_type, default_unit = normalise_data_type(
        conn, instrument_data.source, instrument_data.data_type
    )
    fields = {
        "instrument_id": instrument_data.instrument_id,
        "data_date": _iso(instrument_data.data_date),
        "data_type": canonical_type,
        "value": instrument_data.value,
        "source": instrument_data.source,
        "resolution": instrument_data.resolution,
        "unit": instrument_data.unit or default_unit,
        "attrs": json.dumps(instrument_data.attrs or {}, separators=(",", ":")),
    }

    cols = ", ".join(fields.keys())
    placeholders = ", ".join([f"%({k})s" for k in fields.keys()])
    updates = ", ".join([
        f"{k}=EXCLUDED.{k}"
        for k in fields.keys()
        if k not in ("instrument_id","data_date","data_type","source","resolution")
    ])

    sql = f"""
        INSERT INTO instruments_instrumentdata ({cols})
        VALUES ({placeholders})
        ON CONFLICT (instrument_id, data_date, data_type, source, resolution)
        DO UPDATE SET {updates}
    """
    with conn.cursor() as cur:
        cur.execute(sql, fields)
    return "inserted_or_updated"

