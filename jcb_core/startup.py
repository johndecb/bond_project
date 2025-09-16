import sqlite3
from jcb_bond_project.database.schema import create_instruments_table, create_instrument_data_table
from jcb_bond_project.validate_schema import check_instrument_schema, check_instrument_data_schema

def setup_database(db_path):
    create_instruments_table(db_path)
    create_instrument_data_table(db_path)
    print("✅ Tables created or confirmed.")

def validate_database_schema(db_path):
    conn = sqlite3.connect(db_path)
    check_instrument_schema(conn)
    check_instrument_data_schema(conn)
    conn.close()
    print("✅ Schema validation complete.")