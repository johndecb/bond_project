import argparse
import sqlite3
import pandas as pd
from validate_schema import check_instrument_schema, check_instrument_data_schema
import os
from loaders.load_tradeweb import load_tradeweb_csv

DB_PATH = 'jcb_db.db'

def list_instruments():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT id, short_code, name, instrument_type FROM instruments", conn)
    conn.close()
    print(df.to_string(index=False))

def show_instrument(isin):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM instruments WHERE id = ?", conn, params=(isin,))
    conn.close()
    if df.empty:
        print(f"❌ Instrument with ISIN {isin} not found.")
    else:
        print(df.T.to_string(header=False))  # Pretty vertical format

def show_history(isin):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT data_date, data_type, value FROM instrument_data WHERE instrument_id = ? ORDER BY data_date",
        conn, params=(isin,))
    conn.close()
    if df.empty:
        print(f"❌ No data found for ISIN {isin}.")
    else:
        print(df.to_string(index=False))

def validate_schema():
    conn = sqlite3.connect(DB_PATH)
    check_instrument_schema(conn)
    check_instrument_data_schema(conn)
    conn.close()

def load_tradeweb_cli():
    folder = "tradeweb_data"
    files = [f for f in os.listdir(folder) if f.lower().endswith(".csv") or f.lower().endswith(".xlsx")]

    if not files:
        print("❌ No Tradeweb files found in 'tradeweb_data/'.")
    else:
        print("📁 Available Tradeweb files:")
        for i, f in enumerate(files, 1):
            print(f"{i}: {f}")
        try:
            choice = int(input("Select file number to load: "))
            selected = files[choice - 1]
            print(f"\n📂 Loading: {selected}")
            load_tradeweb_csv(os.path.join(folder, selected))
        except (ValueError, IndexError):
            print("❌ Invalid selection.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI for managing and viewing instrument data.")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("list-instruments", help="List all instruments in the database")

    show_parser = subparsers.add_parser("show-instrument", help="Show details for one instrument")
    show_parser.add_argument("isin", help="ISIN of the instrument")

    hist_parser = subparsers.add_parser("show-history", help="Show time-series data for an instrument")
    hist_parser.add_argument("isin", help="ISIN of the instrument")

    subparsers.add_parser("validate-schema", help="Check if the DB schema is valid")

    subparsers.add_parser("load-tradeweb", help="Load Tradeweb data file from tradeweb_data/")

    args = parser.parse_args()

    if args.command == "list-instruments":
        list_instruments()
    elif args.command == "show-instrument":
        show_instrument(args.isin)
    elif args.command == "show-history":
        show_history(args.isin)
    elif args.command == "validate-schema":
        validate_schema()
    elif args.command == "load-tradeweb":
        load_tradeweb_cli()
    else:
        parser.print_help()
