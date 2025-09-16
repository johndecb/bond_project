from __future__ import annotations
import json
from dataclasses import fields as dc_fields
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

from jcb_bond_project.database.db import get_conn
from jcb_bond_project.models.instrument_data import InstrumentData
from jcb_bond_project.database.insert import insert_instrument_data

DATE_COLS = ["Close of Business Date", "COB Date", "Date"]
ISIN_COLS = ["ISIN", "Instrument", "Instrument ID"]

def _find_col(df: pd.DataFrame, candidates: Iterable[str]) -> str:
    norm = {c.strip().lower(): c for c in df.columns}
    for want in (c.strip().lower() for c in candidates):
        if want in norm:
            return norm[want]
    raise KeyError(f"Could not find any of {candidates} in columns: {list(df.columns)}")

def _parse_ddmmyyyy(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    # 1) strict dd/mm/YYYY
    d = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
    # 2) gentle fallback (handles ISO 'YYYY-MM-DD', still dayfirst=True)
    fb = pd.to_datetime(s, dayfirst=True, errors="coerce")
    d = d.where(d.notna(), fb)
    return d.dt.date

def _fetch_mapping(conn, source: str) -> Dict[str, Tuple[str, Optional[str]]]:
    """
    Returns dict: lower(raw_data_type) -> (canonical_data_type, default_unit)
    Prefers exact source over '*' wildcard.
    """
    # wildcard first, then overwrite with source-specific
    rows_star = conn.execute(
        "SELECT raw_data_type, canonical_data_type, default_unit "
        "FROM data_type_map WHERE source='*'"
    ).fetchall()
    rows_src = conn.execute(
        "SELECT raw_data_type, canonical_data_type, default_unit "
        "FROM data_type_map WHERE LOWER(source)=LOWER(?)",
        (source,),
    ).fetchall()

    m: Dict[str, Tuple[str, Optional[str]]] = {
        r[0].lower(): (r[1], r[2]) for r in rows_star
    }
    m.update({r[0].lower(): (r[1], r[2]) for r in rows_src})
    return m

def load_tradeweb_csv_mapped(
    file_path: str,
    db_path: str = "jcb_db.db",
    *,
    source: str = "Tradeweb",
    resolution: str = "daily_close",
    allow_unmapped: bool = False,   # set True to insert unknown columns as-is
    dry_run: bool = False,
) -> Tuple[int, int, int, int]:
    """
    Load a Tradeweb CSV using data_type_map to canonicalise on insert.
    Returns (inserted, skipped, errors, unmapped_columns_count).
    """
    df = pd.read_csv(file_path)
    df.columns = [c.strip() for c in df.columns]

    isin_col = _find_col(df, ISIN_COLS)
    date_col = _find_col(df, DATE_COLS)

    # vectorised date parse (dd/mm/yyyy + fallback)
    df["_data_date"] = _parse_ddmmyyyy(df[date_col])

    # data columns are everything except id + date
    data_cols = [c for c in df.columns if c not in {isin_col, date_col, "_data_date"}]

    inserted = skipped = errors = 0
    unmapped_cols: set[str] = set()

    with get_conn(db_path) as conn:
        # build mapping once per run
        mapping = _fetch_mapping(conn, source)

        # precompute InstrumentData fields so we can include unit/attrs if available
        id_fields = {f.name for f in dc_fields(InstrumentData)}

        for _, row in df.iterrows():
            isin = str(row[isin_col]).strip()
            d = row["_data_date"]
            if not isin or pd.isna(d):
                continue

            for col in data_cols:
                raw_type = col.strip()
                val = row.get(col)
                if pd.isna(val):
                    continue

                key = raw_type.lower()
                if key in mapping:
                    canonical, unit = mapping[key]
                else:
                    unmapped_cols.add(raw_type)
                    if not allow_unmapped:
                        continue
                    canonical, unit = raw_type, None  # insert raw if allowed

                try:
                    value = float(str(val).replace(",", ""))
                except Exception:
                    continue

                if dry_run:
                    continue

                kwargs = dict(
                    instrument_id=isin,
                    data_date=d,
                    data_type=canonical,
                    value=value,
                    source=source,
                    resolution=resolution,
                )
                # Include unit if InstrumentData supports it
                if "unit" in id_fields and unit:
                    kwargs["unit"] = unit
                # Keep provenance if attrs exists
                if "attrs" in id_fields:
                    kwargs["attrs"] = json.dumps({"raw_type": raw_type})

                try:
                    rec = InstrumentData(**kwargs)
                    res = insert_instrument_data(conn, rec)
                    if res == "inserted":
                        inserted += 1
                    else:
                        skipped += 1
                except Exception:
                    errors += 1

    if unmapped_cols:
        print("⚠️ Unmapped Tradeweb columns (add aliases in data_type_map):")
        for c in sorted(unmapped_cols):
            print("   -", c)

    print(f"✅ Load complete: {inserted} inserted, {skipped} skipped, {errors} errors.")
    return inserted, skipped, errors, len(unmapped_cols)
