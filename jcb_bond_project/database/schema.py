import sqlite3

def create_instruments_table(conn: sqlite3.Connection):
    
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='instruments';
    """)
    exists = cursor.fetchone() is not None

    if exists:
        print("✅ Table 'instruments' already exists.")
    else:
        cursor.execute('''
            CREATE TABLE instruments (
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
            )
        ''')
        print("ℹ️ Table 'instruments' created.")

    return exists

def create_instrument_data_table(conn: sqlite3.Connection):
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='instrument_data';
    """)
    exists = cursor.fetchone() is not None

    if exists:
        print("✅ Table 'instrument_data' already exists.")
    else:
        cursor.execute('''
            CREATE TABLE instrument_data (
                instrument_id TEXT NOT NULL,
                data_date DATE NOT NULL,
                data_type TEXT NOT NULL,
                value REAL,
                source TEXT,
                resolution TEXT,
                PRIMARY KEY (instrument_id, data_date, data_type, source, resolution),
                FOREIGN KEY (instrument_id) REFERENCES instruments(isin)
            )
            ''')
        print("ℹ️ Table 'instrument_data' created.")

    return exists

def create_instrument_identifier_table(conn: sqlite3.Connection):

    cursor = conn.cursor()
    # Check if the table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='instrument_identifier';
    """)
    exists = cursor.fetchone() is not None

    if exists:
        print("✅ Table 'instrument_identifier' already exists.")
    else:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS instrument_identifier (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_id TEXT,
                identifier_string TEXT,
                identifier_source TEXT,
                UNIQUE (instrument_id, identifier_string, identifier_source),
                FOREIGN KEY (instrument_id) REFERENCES instruments(id) ON DELETE CASCADE
            )
            ''')
        print("ℹ️ Table 'instrument_identifier' created.")

    return exists

def create_calendar_holidays_table(conn: sqlite3.Connection):
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS calendar_holidays (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calendar_name TEXT NOT NULL,
            holiday_date DATE NOT NULL,
            description TEXT,
            UNIQUE(calendar_name, holiday_date)
        )
    ''')
    print("✅ Table 'calendar_holidays' created or already exists.")