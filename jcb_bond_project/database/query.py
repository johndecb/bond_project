from __future__ import annotations
import sqlite3, json
import pandas as pd
from dataclasses import fields
from datetime import date
from typing import Optional, Sequence, Dict, Any, Union, List
from jcb_bond_project.jcb_bond_project.models.instrument import Instrument

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
        # handles 'YYYY-MM-DD...' strings
        return date.fromisoformat(str(x)[:10])
    except Exception:
        return None


def _row_to_instrument(row_tuple, col_names) -> Instrument:
    data = dict(zip(col_names, row_tuple))

    # normalise common types
    for k in ("is_green", "is_linker"):
        if k in data:
            data[k] = _coerce_bool(data[k])

    for k in ("maturity_date", "first_issue_date", "issue_date"):
        if k in data:
            data[k] = _coerce_date(data[k])

    # keep only fields defined on the dataclass
    inst_field_names = {f.name for f in fields(Instrument)}
    kwargs = {k: v for k, v in data.items() if k in inst_field_names}
    return Instrument(**kwargs)

def load_instrument_data(
    conn: sqlite3.Connection,
    instrument_id: str,
    source: Optional[Union[str, Sequence[str]]] = None,
    data_type: Optional[Union[str, Sequence[str]]] = None,
    start_date: Optional[Union[str, date]] = None,
    end_date: Optional[Union[str, date]] = None,
    *,
    resolution: Optional[str] = None,
    unit: Optional[str] = None,
    # Common JSON attrs as first-class args:
    session: Optional[str] = None,          # e.g. "close", "open", "settlement"
    quote_side: Optional[str] = None,       # e.g. "mid","bid","ask"
    attrs_filters: Optional[Dict[str, Any]] = None,  # any extra JSON attrs
    long_format: bool = True,               # False -> pivot wide by data_type
    parse_dates: bool = True
) -> pd.DataFrame:
    """
    Load instrument_data with flexible filters, including JSON attrs.
    Returns a tidy (long) DataFrame by default, or pivoted wide by data_type.
    """
    # Normalize inputs
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

    # Detect JSON1 availability
    json1_ok = True
    try:
        conn.execute("SELECT json('{}');").fetchone()
    except sqlite3.OperationalError:
        json1_ok = False

    # Build SQL
    sql = """
      SELECT
        data_date,
        data_type,
        value,
        source,
        resolution,
        unit,
        attrs
      FROM instrument_data
      WHERE instrument_id = ?
    """
    params = [instrument_id]

    if sources:
        sql += " AND source IN ({})".format(",".join(["?"] * len(sources)))
        params.extend(list(sources))
    if data_types:
        sql += " AND data_type IN ({})".format(",".join(["?"] * len(data_types)))
        params.extend(list(data_types))
    if resolution:
        sql += " AND resolution = ?"
        params.append(resolution)
    if unit:
        sql += " AND unit = ?"
        params.append(unit)
    if start_iso:
        sql += " AND data_date >= ?"
        params.append(start_iso)
    if end_iso:
        sql += " AND data_date <= ?"
        params.append(end_iso)

    # JSON filters (prefer SQL if JSON1 is available)
    if json1_ok and extra_attrs:
        for k, v in extra_attrs.items():
            sql += f" AND json_extract(attrs, '$.{k}') = ?"
            params.append(v)

    sql += " ORDER BY data_date"

    df = pd.read_sql_query(sql, conn, params=params)

    # If JSON1 wasn’t available, filter attrs in Python
    if not df.empty:
        # Parse attrs column to dict
        def _parse(x):
            try:
                return json.loads(x) if isinstance(x, str) and x else {}
            except Exception:
                return {}
        df["attrs"] = df["attrs"].apply(_parse)

        if not json1_ok and extra_attrs:
            for k, v in extra_attrs.items():
                df = df[df["attrs"].apply(lambda d: d.get(k) == v)]

        # Extract a few common attrs as columns (optional, handy)
        for k in ("session", "quote_side", "settlement_date"):
            if k not in df.columns:
                df[k] = df["attrs"].apply(lambda d: d.get(k))

        if parse_dates:
            df["data_date"] = pd.to_datetime(df["data_date"], errors="coerce").dt.date

        # Wide pivot if requested (assumes single source/resolution/unit per date)
        if not long_format:
            wide = df.pivot_table(index="data_date",
                                  columns="data_type",
                                  values="value",
                                  aggfunc="last").sort_index()
            # Give the columns a flat index
            wide.columns.name = None
            return wide

    return df



# Define this once so INSERT/SELECT stay in sync
_INSTR_COLUMNS = (
    "id, short_code, name, instrument_type, issuer, country, currency, "
    "maturity_date, first_issue_date, coupon_rate, first_coupon_length, "
    "is_green, is_linker, index_lag, rpi_base, tenor, reference_index, day_count_fraction"
)

def _iso(d: Any) -> Optional[str]:
    if d is None: return None
    return d.isoformat() if isinstance(d, date) else str(d)

def get_instrument(conn, isin: str) -> Optional[Instrument]:
    row = conn.execute("SELECT * FROM instruments WHERE isin = ? LIMIT 1", (isin,)).fetchone()
    return _row_to_instrument(row) if row else None

def list_instruments(
    conn: sqlite3.Connection,
    instrument_types: Optional[Sequence[str]] = None,
    country: Optional[str] = None,
    is_green: Optional[bool] = None,
    is_linker: Optional[bool] = None,
    like: Optional[str] = None,          # substring filter
    columns: Optional[Sequence[str]] = None,  # choose specific cols
    _debug: bool = False,
) -> pd.DataFrame:
    """
    Return a DataFrame of instruments with optional filters.
    Schema columns (key ones): isin, short_code, name, instrument_type, issuer, country,
    currency, maturity_date, first_issue_date, coupon_rate, is_green, is_linker, ...
    """
    # default visible columns
    if not columns:
        columns = [
            "isin", "short_code", "name", "instrument_type",
            "issuer", "country", "currency",
            "maturity_date", "first_issue_date",
            "coupon_rate", "is_green", "is_linker"
        ]

    sql = f"""
        SELECT {", ".join(columns)}
        FROM instruments
        WHERE 1=1
    """
    params: list = []

    # filters
    if instrument_types:
        placeholders = ",".join(["?"] * len(instrument_types))
        sql += f" AND instrument_type IN ({placeholders})"
        params.extend(instrument_types)

    if country:
        sql += " AND country = ?"
        params.append(country)

    if is_green is not None:
        sql += " AND is_green = ?"
        params.append(1 if is_green else 0)

    if is_linker is not None:
        sql += " AND is_linker = ?"
        params.append(1 if is_linker else 0)

    if like:
        sql += " AND (name LIKE ? OR short_code LIKE ? OR isin LIKE ?)"
        needle = f"%{like}%"
        params.extend([needle, needle, needle])

    sql += " ORDER BY instrument_type, name, isin"

    if _debug:
        print("SQL:", sql)
        print("PARAMS:", params)

    df = pd.read_sql_query(sql, conn, params=params)

    # normalise booleans if present
    for bcol in ("is_green", "is_linker"):
        if bcol in df.columns:
            df[bcol] = df[bcol].astype("Int64").map({0: False, 1: True})
    # dates come back as strings; leave as-is or parse:
    # for dcol in ("maturity_date", "first_issue_date"):
    #     if dcol in df.columns:
    #         df[dcol] = pd.to_datetime(df[dcol], errors="coerce").dt.date

    return df

def list_instruments(
    conn: sqlite3.Connection,
    *,
    instrument_types: Optional[Sequence[str]] = None,
    country: Optional[str] = None,
    is_green: Optional[bool] = None,
    is_linker: Optional[bool] = None,
    like: Optional[str] = None,
    order_by: str = "instrument_type, name, isin",
    limit: Optional[int] = None,
) -> List[Instrument]:
    """
    Return a list of Instrument objects matching the filters.
    """
    sql_parts = ["SELECT * FROM instruments WHERE 1=1"]
    params: list = []

    if instrument_types:
        sql_parts.append(f"AND instrument_type IN ({','.join('?' for _ in instrument_types)})")
        params.extend(instrument_types)

    if country:
        sql_parts.append("AND country = ?")
        params.append(country)

    if is_green is not None:
        sql_parts.append("AND is_green = ?")
        params.append(1 if is_green else 0)

    if is_linker is not None:
        sql_parts.append("AND is_linker = ?")
        params.append(1 if is_linker else 0)

    if like:
        sql_parts.append("AND (name LIKE ? OR short_code LIKE ? OR isin LIKE ?)")
        needle = f"%{like}%"
        params.extend([needle, needle, needle])

    sql_parts.append(f"ORDER BY {order_by}")
    if limit:
        sql_parts.append("LIMIT ?")
        params.append(int(limit))

    query = " ".join(sql_parts)
    cur = conn.execute(query, params)
    rows = cur.fetchall()
    col_names = [d[0] for d in cur.description]

    return [_row_to_instrument(r, col_names) for r in rows]

def resolve_isin_from_alt_id(conn: sqlite3.Connection, alt_id: str, source: str = "Bloomberg") -> str:
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT instrument_id FROM instrument_identifiers
        WHERE identifier_string = ? AND identifier_source = ?
    """, (alt_id, source))
    result = cursor.fetchone()
    return result[0] if result else None

def _to_date(v) -> date:
    if isinstance(v, date):
        return v
    if v is None or v == "":
        raise ValueError("holiday_date is NULL/empty")
    # SQLite often returns TEXT 'YYYY-MM-DD'
    return date.fromisoformat(str(v))

# database/query.py
from typing import List
import sqlite3
from datetime import date as _date

def get_holidays_for_calendar(conn: sqlite3.Connection, calendar_name: str, _debug: bool = False) -> List[_date]:
    sql = (
        "SELECT holiday_date FROM calendar_holidays "
        "WHERE calendar_name = ? "
        "ORDER BY holiday_date"
    )
    params = (calendar_name,)
    if _debug:
        print("SQL:", sql, "\nparams:", params)

    rows = conn.execute(sql, params).fetchall()
    # If row_factory=sqlite3.Row (as in your connect()), both r[0] and r["holiday_date"] work:
    return [_to_date(r["holiday_date"]) for r in rows]  # or r[0]

def inspect_schema(conn, save_path: str = None):
    """
    Inspect SQLite schema: print, return dict, and optionally save to file.
    
    Args:
        conn: sqlite3.Connection
        save_path: optional path to save schema as text file
    
    Returns:
        dict with structure {table_name: [{"name": col_name, "type": col_type, "pk": bool, "notnull": bool, "default": value}, ...]}
    """
    cursor = conn.cursor()

    # List all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    schema = {}
    output_lines = ["📋 Tables in database:"]

    for table in tables:
        output_lines.append(f"\n== {table} ==")
        cursor.execute(f"PRAGMA table_info({table});")
        columns = []
        for cid, name, col_type, notnull, default, pk in cursor.fetchall():
            col_info = {
                "name": name,
                "type": col_type,
                "pk": bool(pk),
                "notnull": bool(notnull),
                "default": default,
            }
            columns.append(col_info)
            output_lines.append(f" - {name} ({col_type}){' [PK]' if pk else ''}")
        schema[table] = columns

    # Print to console
    print("\n".join(output_lines))

    # Save to file if requested
    if save_path:
        with open(save_path, "w") as f:
            f.write("\n".join(output_lines))
        print(f"\n💾 Schema saved to {save_path}")

    return schema