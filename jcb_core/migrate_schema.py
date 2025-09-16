import os
import shutil
import sqlite3
from datetime import datetime

DB_PATH = "jcb_db.db"  # adjust if needed

# ---- Helpers ---------------------------------------------------------------

def backup_db(db_path: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup_path = f"{db_path}.bak.migrate.{ts}"
    shutil.copy2(db_path, backup_path)
    print(f"📦 Backup created: {backup_path}")
    return backup_path

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;",
        (name,),
    )
    return cur.fetchone() is not None

def columns_of(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [row[1] for row in cur.fetchall()]

def safe_val(row: sqlite3.Row, col: str, default=None):
    return row[col] if col in row.keys() else default

# ---- Migration plan --------------------------------------------------------

def migrate(db_path: str):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")

    backup_db(db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Safety: control FKs during table moves
        conn.execute("PRAGMA foreign_keys = OFF;")
        conn.execute("PRAGMA journal_mode = WAL;")

        with conn:  # transaction
            # --- 1) Create NEW tables with desired schema -------------------
            # instruments (use natural PK = isin)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS instruments_new (
                    isin TEXT PRIMARY KEY,
                    short_code TEXT,
                    name TEXT,
                    instrument_type TEXT,
                    issuer TEXT,
                    country TEXT,
                    currency TEXT,
                    maturity_date DATE,
                    first_issue_date DATE,
                    coupon_rate REAL,
                    first_coupon_length TEXT,
                    is_green BOOLEAN,
                    is_linker BOOLEAN,
                    index_lag INTEGER,
                    rpi_base REAL,
                    tenor TEXT,
                    reference_index TEXT,
                    day_count_fraction TEXT
                );
            """)

            # instrument_data (composite PK + unit + attrs)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS instrument_data_new (
                    instrument_id TEXT NOT NULL,
                    data_date DATE NOT NULL,
                    data_type TEXT NOT NULL,
                    value REAL,
                    source TEXT,
                    resolution TEXT,
                    unit TEXT,
                    attrs TEXT,
                    PRIMARY KEY (instrument_id, data_date, data_type, source, resolution),
                    FOREIGN KEY (instrument_id) REFERENCES instruments_new(isin)
                );
            """)

            # instrument_identifiers (plural; natural PK)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS instrument_identifiers_new (
                    instrument_id TEXT NOT NULL,
                    identifier_string TEXT NOT NULL,
                    identifier_source TEXT NOT NULL,
                    PRIMARY KEY (instrument_id, identifier_string, identifier_source),
                    FOREIGN KEY (instrument_id) REFERENCES instruments_new(isin) ON DELETE CASCADE
                );
            """)

            # calendar_holidays unchanged
            conn.execute("""
                CREATE TABLE IF NOT EXISTS calendar_holidays_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    calendar_name TEXT NOT NULL,
                    holiday_date DATE NOT NULL,
                    description TEXT,
                    UNIQUE(calendar_name, holiday_date)
                );
            """)

            # --- 2) Copy data from old tables if present --------------------
            # instruments -> instruments_new (map id -> isin)
            if table_exists(conn, "instruments"):
                print("➡️  Migrating table: instruments -> instruments_new")
                old_cols = set(columns_of(conn, "instruments"))
                cur = conn.execute("SELECT * FROM instruments;")
                rows = cur.fetchall()
                for r in rows:
                    isin = safe_val(r, "isin", None) or safe_val(r, "id", None)
                    if not isin:
                        continue  # skip malformed
                    conn.execute("""
                        INSERT OR REPLACE INTO instruments_new
                        (isin, short_code, name, instrument_type, issuer, country, currency,
                         maturity_date, first_issue_date, coupon_rate, first_coupon_length,
                         is_green, is_linker, index_lag, rpi_base, tenor, reference_index, day_count_fraction)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        isin,
                        safe_val(r, "short_code"),
                        safe_val(r, "name"),
                        safe_val(r, "instrument_type"),
                        safe_val(r, "issuer"),
                        safe_val(r, "country"),
                        safe_val(r, "currency"),
                        safe_val(r, "maturity_date"),
                        safe_val(r, "first_issue_date"),
                        safe_val(r, "coupon_rate"),
                        safe_val(r, "first_coupon_length"),
                        safe_val(r, "is_green"),
                        safe_val(r, "is_linker"),
                        safe_val(r, "index_lag"),
                        safe_val(r, "rpi_base"),
                        safe_val(r, "tenor"),
                        safe_val(r, "reference_index"),
                        safe_val(r, "day_count_fraction"),
                    ))
            else:
                print("ℹ️  Old table 'instruments' not found; skipping copy.")

            # instrument_data -> instrument_data_new
            if table_exists(conn, "instrument_data"):
                print("➡️  Migrating table: instrument_data -> instrument_data_new")
                old_cols = set(columns_of(conn, "instrument_data"))
                cur = conn.execute("SELECT * FROM instrument_data;")
                rows = cur.fetchall()
                # Guess legacy column names; handle both old/new
                for r in rows:
                    instrument_id = safe_val(r, "instrument_id") or safe_val(r, "isin") or safe_val(r, "id")
                    if not instrument_id:
                        continue
                    data_date = safe_val(r, "data_date")
                    data_type = safe_val(r, "data_type")
                    value     = safe_val(r, "value")
                    source    = safe_val(r, "source")
                    resolution= safe_val(r, "resolution")
                    unit      = safe_val(r, "unit")      # may not exist in old schema
                    attrs     = safe_val(r, "attrs")     # may not exist in old schema

                    conn.execute("""
                        INSERT OR REPLACE INTO instrument_data_new
                        (instrument_id, data_date, data_type, value, source, resolution, unit, attrs)
                        VALUES (?,?,?,?,?,?,?,?)
                    """, (instrument_id, data_date, data_type, value, source, resolution, unit, attrs))
            else:
                print("ℹ️  Old table 'instrument_data' not found; skipping copy.")

            # instrument_identifier -> instrument_identifiers_new
            if table_exists(conn, "instrument_identifier"):
                print("➡️  Migrating table: instrument_identifier -> instrument_identifiers_new")
                cur = conn.execute("SELECT * FROM instrument_identifier;")
                rows = cur.fetchall()
                for r in rows:
                    instrument_id = safe_val(r, "instrument_id")
                    identifier_string = safe_val(r, "identifier_string")
                    identifier_source = safe_val(r, "identifier_source")
                    if not (instrument_id and identifier_string and identifier_source):
                        continue
                    conn.execute("""
                        INSERT OR REPLACE INTO instrument_identifiers_new
                        (instrument_id, identifier_string, identifier_source)
                        VALUES (?,?,?)
                    """, (instrument_id, identifier_string, identifier_source))
            else:
                print("ℹ️  Old table 'instrument_identifier' not found; skipping copy.")

            # calendar_holidays -> calendar_holidays_new
            if table_exists(conn, "calendar_holidays"):
                print("➡️  Migrating table: calendar_holidays -> calendar_holidays_new")
                cur = conn.execute("SELECT * FROM calendar_holidays;")
                rows = cur.fetchall()
                for r in rows:
                    conn.execute("""
                        INSERT OR IGNORE INTO calendar_holidays_new
                        (id, calendar_name, holiday_date, description)
                        VALUES (?,?,?,?)
                    """, (safe_val(r, "id"),
                          safe_val(r, "calendar_name"),
                          safe_val(r, "holiday_date"),
                          safe_val(r, "description")))
            else:
                print("ℹ️  Old table 'calendar_holidays' not found; skipping copy.")

            # --- 3) Drop old tables & rename NEW -> canonical ----------------
            if table_exists(conn, "instrument_data"):
                conn.execute("DROP TABLE instrument_data;")
            if table_exists(conn, "instruments"):
                conn.execute("DROP TABLE instruments;")
            if table_exists(conn, "instrument_identifier"):
                conn.execute("DROP TABLE instrument_identifier;")
            if table_exists(conn, "calendar_holidays"):
                conn.execute("DROP TABLE calendar_holidays;")

            conn.execute("ALTER TABLE instruments_new RENAME TO instruments;")
            conn.execute("ALTER TABLE instrument_data_new RENAME TO instrument_data;")
            conn.execute("ALTER TABLE instrument_identifiers_new RENAME TO instrument_identifiers;")
            conn.execute("ALTER TABLE calendar_holidays_new RENAME TO calendar_holidays;")

            # --- 4) Indexes -------------------------------------------------
            # Helpful indexes for common queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_instr_currency ON instruments(currency);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_instr_type ON instruments(instrument_type);")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_data_instr_date ON instrument_data(instrument_id, data_date);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_data_type ON instrument_data(data_type);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_data_source ON instrument_data(source);")

        # Re-enable FKs after migration and checkpoint WAL
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA wal_checkpoint(FULL);")
        print("✅ Migration complete.")

    finally:
        conn.close()

if __name__ == "__main__":
    migrate(DB_PATH)
