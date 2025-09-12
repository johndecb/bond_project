# database/normalise_types.py
from __future__ import annotations
import sqlite3
from typing import Iterable, Tuple

def ensure_map_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_type_map (
            source TEXT NOT NULL,
            raw_data_type TEXT NOT NULL,
            canonical_data_type TEXT NOT NULL,
            default_unit TEXT,
            note TEXT,
            PRIMARY KEY (source, raw_data_type)
        );
    """)
    # helpful indexes for the normaliser join
    conn.execute("CREATE INDEX IF NOT EXISTS ix_dtm_source ON data_type_map(source);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_dtm_raw ON data_type_map(raw_data_type);")

SEED_MAPPINGS: Iterable[Tuple[str,str,str,str,str]] = [
    # source,     raw_data_type            -> canonical,           unit,      note
    ("Bloomberg", "PX_LAST",                "clean_price",         "per_100", "Govt/Corp clean"),
    ("Bloomberg", "DIRTY_PRICE",            "dirty_price",         "per_100", ""),
    ("Bloomberg", "YLD_YTM_LAST",           "yield",               "percent", ""),
    ("Bloomberg", "MOD_DUR",                "modified_duration",   "years",   ""),
    ("Bloomberg", "ACC_INT",                "accrued_interest",    "per_100", ""),
    ("Bloomberg", "Z_SPRD",                 "z_spread",            "bps",     ""),

    # Tradeweb variants (add both common spellings)
    ("Tradeweb",  "Clean Price",            "clean_price",         "per_100", ""),
    ("Tradeweb",  "Dirty Price",            "dirty_price",         "per_100", ""),
    ("Tradeweb",  "Yield",                  "yield",               "percent", "alias for Yield Mid"),
    ("Tradeweb",  "Yield Mid",              "yield",               "percent", ""),
    ("Tradeweb",  "Mod Duration",           "modified_duration",   "years",   ""),
    ("Tradeweb",  "Modified Duration",      "modified_duration",   "years",   ""),
    ("Tradeweb",  "Accrued Interest",       "accrued_interest",    "per_100", ""),

    # DMO
    ("DMO",       "Clean Price",            "clean_price",         "per_100", ""),
    ("DMO",       "Yield",                  "yield",               "percent", ""),
    ("DMO",       "Accrued Interest",       "accrued_interest",    "per_100", ""),

    # Wildcards / historical oddities
    ("*",         "price_clean",            "clean_price",         "per_100", ""),
    ("*",         "cleanprice",             "clean_price",         "per_100", ""),
    ("*",         "price_dirty",            "dirty_price",         "per_100", ""),
    ("*",         "dirtyprice",             "dirty_price",         "per_100", ""),
    ("*",         "mod_duration",           "modified_duration",   "years",   ""),
    ("*",         "rpi",                    "rpi",                 "index",   ""),
]

def seed_mappings(conn: sqlite3.Connection, rows=SEED_MAPPINGS):
    ensure_map_table(conn)
    conn.executemany("""
        INSERT OR REPLACE INTO data_type_map
        (source, raw_data_type, canonical_data_type, default_unit, note)
        VALUES (?, ?, ?, ?, ?);
    """, list(rows))

def normalise_instrument_data(conn: sqlite3.Connection):
    conn.execute("PRAGMA foreign_keys = OFF;")
    conn.executescript("""
        DROP TABLE IF EXISTS instrument_data_norm;

        CREATE TABLE instrument_data_norm (
            instrument_id TEXT NOT NULL,
            data_date DATE NOT NULL,
            data_type TEXT NOT NULL,
            value REAL,
            source TEXT,
            resolution TEXT,
            unit TEXT,
            attrs TEXT,
            PRIMARY KEY (instrument_id, data_date, data_type, source, resolution)
        );

        -- Prefer exact source mapping; fallback to '*' wildcard.
        -- Make both source and raw_data_type case-insensitive.
        WITH mapped AS (
            SELECT
                d.instrument_id,
                d.data_date,
                COALESCE(m1.canonical_data_type, m2.canonical_data_type, d.data_type) AS canonical_type,
                d.value,
                d.source,
                d.resolution,
                COALESCE(d.unit, m1.default_unit, m2.default_unit) AS unit,
                d.attrs
            FROM instrument_data d
            LEFT JOIN data_type_map m1
              ON LOWER(m1.source) = LOWER(d.source)
             AND LOWER(m1.raw_data_type) = LOWER(d.data_type)
            LEFT JOIN data_type_map m2
              ON m2.source = '*'
             AND LOWER(m2.raw_data_type) = LOWER(d.data_type)
        )
        INSERT INTO instrument_data_norm
        SELECT
            instrument_id,
            data_date,
            canonical_type AS data_type,
            MAX(value)     AS value,
            source,
            resolution,
            MAX(unit)      AS unit,
            MAX(attrs)     AS attrs
        FROM mapped
        GROUP BY instrument_id, data_date, canonical_type, source, resolution;
    """)

def swap_instrument_data(conn: sqlite3.Connection):
    conn.executescript("""
        ALTER TABLE instrument_data RENAME TO instrument_data_old;
        ALTER TABLE instrument_data_norm RENAME TO instrument_data;
        DROP TABLE instrument_data_old;
    """)

def create_audit_view(conn: sqlite3.Connection):
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_data_type_counts AS
        SELECT source, data_type, COUNT(*) AS n
        FROM instrument_data
        GROUP BY source, data_type
        ORDER BY source, data_type;
    """)
