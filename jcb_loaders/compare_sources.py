# etl/compare_sources.py
from __future__ import annotations
from typing import Optional, Sequence
import pandas as pd
from jcb_bond_project.database.db import get_conn

def compare_source_series(
    db_path: str,
    isin: str,
    data_type: str = "clean_price",
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sources: Sequence[str] = ("Bloomberg", "Tradeweb"),
    use_normalised: bool = True,
) -> pd.DataFrame:
    table = "instrument_data" if use_normalised else "instrument_data"
    with get_conn(db_path) as conn:
        sql = f"""
        SELECT data_date, source, value
        FROM {table}
        WHERE instrument_id = ?
          AND data_type = ?
        """
        params = [isin, data_type]
        if start:
            sql += " AND data_date >= ?"; params.append(start)
        if end:
            sql += " AND data_date <= ?"; params.append(end)
        if sources:
            sql += " AND source IN ({})".format(",".join("?" for _ in sources))
            params.extend(sources)
        df = pd.read_sql_query(sql, conn, params=params, parse_dates=["data_date"])

    if df.empty:
        return df

    pivot = df.pivot_table(index="data_date", columns="source", values="value", aggfunc="last").sort_index()
    if set(sources).issubset(pivot.columns):
        pivot["diff"] = pivot[sources[1]] - pivot[sources[0]]
        # useful "bp" view for yields and prices
        if data_type in {"yield", "clean_price", "dirty_price"}:
            pivot["diff_bp"] = pivot["diff"] * 100.0
    return pivot
