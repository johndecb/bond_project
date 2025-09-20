# database/query.py

from __future__ import annotations

import json
import pandas as pd
from dataclasses import fields
from datetime import date
from typing import Optional, Sequence, Dict, Any, Union, List
import psycopg2
from psycopg2.extras import RealDictCursor

from jcb_bond_project.models.instrument import Instrument


# ------------ helpers ------------

def _coerce_bool(x):
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    try:
        return bool(int(x))
    except Exception:
        return bool(x)

def _coerce_date(x):
    if x is None or isinstance(x, date):
        return x
    try:
        return date.fromisoformat(str(x)[:10])
    except Exception:
        return None

def _row_to_instrument(row_dict: Dict[str, Any]) -> Instrument:
    """Convert DB row dict -> Instrument dataclass"""
    data = dict(row_dict)

    for k in ("is_green", "is_linker"):
        if k in data:
            data[k] = _coerce_bool(data[k])

    for k in ("maturity_date", "first_issue_date", "issue_date"):
        if k in data:
            data[k] = _coerce_date(data[k])

    inst_field_names = {f.name for f in fields(Instrument)}
    kwargs = {k: v for k, v in data.items() if k in inst_field_names}
    return Instrument(**kwargs)


# ------------ instrument_data ------------

def load_instrument_data(
    conn,
    instrument_id: str,
    source: Optional[Union[str, Sequence[str]]] = None,
    data_type: Optional[Union[str, Sequence[str]]] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    *,
    resolution: Optional[str] = None,
    unit: Optional[str] = None,
    session: Optional[str] = None,
    quote_side: Optional[str] = None,
    attrs_filters: Optional[Dict[str, Any]] = None,
    long_format: bool = True,
    parse_dates: bool = True,
) -> pd.DataFrame:
    """
    Load instrument_data with flexible filters, including JSON attrs.
    Postgres version.
    """
    def _to_list(x):
        if x is None or isinstance(x, (list, tuple, set)):
            return x
        return [x]

    data_types = _to_list(data_type)
    sources = _to_list(source)
    extra_attrs = attrs_filters.copy() if attrs_filters else {}
    if session is not None:
        extra_attrs["session"] = session
    if quote_side is not None:
        extra_attrs["quote_side"] = quote_side

    def _iso(x):
        if x is None:
            return None
        if isinstance(x, date):
            return x.isoformat()
        return str(x)

    start_iso = _iso(start_date)
    end_iso = _iso(end_date)

    sql = """
      SELECT
        data_date,
        data_type,
        value,
        source,
        resolution,
        unit,
        attrs
      FROM instruments_instrumentdata
      WHERE instrument_id = %s
    """
    params = [instrument_id]

    if sources:
        sql += " AND source = ANY(%s)"
        params.append(sources)
    if data_types:
        sql += " AND data_type = ANY(%s)"
        params.append(data_types)
    if resolution:
        sql += " AND resolution = %s"
        params.append(resolution)
    if unit:
        sql += " AND unit = %s"
        params.append(unit)
    if start_iso:
        sql += " AND data_date >= %s"
        params.append(start_iso)
    if end_iso:
        sql += " AND data_date <= %s"
        params.append(end_iso)

    # JSON filters (Postgres jsonb @> operator)
    for k, v in extra_attrs.items():
        sql += f" AND attrs ->> %s = %s"
        params.extend([k, str(v)])

    sql += " ORDER BY data_date"

    df = pd.read_sql_query(sql, conn, params=params)

    if not df.empty:
        # Parse attrs JSON into dict
        df["attrs"] = df["attrs"].apply(lambda x: x if isinstance(x, dict) else json.loads(x or "{}"))

        if parse_dates:
            df["data_date"] = pd.to_datetime(df["data_date"], errors="coerce").dt.date

        if not long_format:
            wide = df.pivot_table(
                index="data_date",
                columns="data_type",
                values="value",
                aggfunc="last"
            ).sort_index()
            wide.columns.name = None
            return wide

    return df


# ------------ instruments ------------

def get_instrument(conn, isin: str) -> Optional[Instrument]:
    sql = "SELECT * FROM instruments_instrument WHERE isin = %s LIMIT 1"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (isin,))
        row = cur.fetchone()
    return _row_to_instrument(row) if row else None


def list_instruments(
    conn,
    *,
    instrument_types: Optional[Sequence[str]] = None,
    country: Optional[str] = None,
    is_green: Optional[bool] = None,
    is_linker: Optional[bool] = None,
    like: Optional[str] = None,
    order_by: str = "instrument_type, name, isin",
    limit: Optional[int] = None,
) -> List[Instrument]:
    sql_parts = ["SELECT * FROM instruments_instrument WHERE 1=1"]
    params: list = []

    if instrument_types:
        sql_parts.append("AND instrument_type = ANY(%s)")
        params.append(instrument_types)

    if country:
        sql_parts.append("AND country = %s")
        params.append(country)

    if is_green is not None:
        sql_parts.append("AND is_green = %s")
        params.append(is_green)

    if is_linker is not None:
        sql_parts.append("AND is_linker = %s")
        params.append(is_linker)

    if like:
        sql_parts.append("AND (name ILIKE %s OR short_code ILIKE %s OR isin ILIKE %s)")
        needle = f"%{like}%"
        params.extend([needle, needle, needle])

    sql_parts.append(f"ORDER BY {order_by}")
    if limit:
        sql_parts.append("LIMIT %s")
        params.append(int(limit))

    query = " ".join(sql_parts)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [_row_to_instrument(r) for r in rows]


# ------------ alt IDs ------------

def resolve_isin_from_alt_id(conn, alt_id: str, source: str = "Bloomberg") -> Optional[str]:
    sql = """
        SELECT instrument_id
        FROM instruments_instrumentidentifier
        WHERE identifier_string = %s AND identifier_source = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (alt_id, source))
        row = cur.fetchone()
    return row[0] if row else None


# ------------ holidays ------------

def get_holidays_for_calendar(conn, calendar_name: str, _debug: bool = False) -> List[date]:
    sql = """
        SELECT holiday_date
        FROM instruments_calendarholiday
        WHERE calendar_name = %s
        ORDER BY holiday_date
    """
    with conn.cursor() as cur:
        cur.execute(sql, (calendar_name,))
        rows = cur.fetchall()
    return [r[0] for r in rows]
