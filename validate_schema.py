import sqlite3
from models.instrument import Instrument

def check_instrument_schema(conn):
    cursor = conn.cursor()

    # Get column names from the 'instruments' table
    cursor.execute("PRAGMA table_info(instruments);")
    table_columns = [row[1] for row in cursor.fetchall()]

    # Define the expected schema
    expected_columns = [
        'id', 'short_code', 'name', 'instrument_type',
        'issuer', 'country', 'currency',
        'maturity_date', 'first_issue_date',
        'coupon_rate', 'is_green', 'is_linker',
        'index_lag', 'rpi_base'
    ]

    missing = [col for col in expected_columns if col not in table_columns]
    extra = [col for col in table_columns if col not in expected_columns]

    if missing:
        print(f"❌ instruments is missing columns: {missing}")
    if extra:
        print(f"⚠️ instruments has unexpected columns: {extra}")
    if not missing and not extra:
        print("✅ instruments table schema looks correct.")

def check_instrument_data_schema(conn):
    expected_columns = {
        "id", "instrument_id", "data_date", "data_type",
        "value", "source", "resolution"
    }

    cursor = conn.cursor()

    # Check column names
    cursor.execute("PRAGMA table_info(instrument_data)")
    columns = {row[1] for row in cursor.fetchall()}

    missing = expected_columns - columns
    extra = columns - expected_columns

    if missing:
        print(f"❌ instrument_data is missing columns: {missing}")
    if extra:
        print(f"⚠️ instrument_data has unexpected columns: {extra}")
    if not missing and not extra:
        print("✅ instrument_data table schema looks correct.")