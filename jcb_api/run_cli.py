import os
import argparse
import django
import pandas as pd

# --- bootstrap Django ---
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "funderly.settings")
django.setup()

from jcb_bond_project.models import Instrument, InstrumentData


# ------------ commands ------------

def list_instruments():
    qs = Instrument.objects.all().values("isin", "short_code", "name", "instrument_type")
    df = pd.DataFrame.from_records(qs)
    if df.empty:
        print("⚠️ No instruments found")
    else:
        print(df.to_string(index=False))


def show_instrument(isin):
    try:
        inst = Instrument.objects.get(isin=isin)
        df = pd.DataFrame(inst.__dict__.items(), columns=["Field", "Value"])
        print(df.to_string(index=False, header=False))
    except Instrument.DoesNotExist:
        print(f"❌ Instrument with ISIN {isin} not found.")


def show_history(isin):
    qs = InstrumentData.objects.filter(instrument_id=isin).order_by("data_date")
    df = pd.DataFrame.from_records(qs.values("data_date", "data_type", "value"))
    if df.empty:
        print(f"❌ No data found for ISIN {isin}.")
    else:
        print(df.to_string(index=False))


def validate_schema():
    print("ℹ️ Using Django ORM schema validation")
    # This checks if migrations match the database
    from django.core.management import call_command
    call_command("migrate", check=True, plan=True)


# ------------ main ------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI for managing and viewing instrument data.")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("list-instruments", help="List all instruments in the database")

    show_parser = subparsers.add_parser("show-instrument", help="Show details for one instrument")
    show_parser.add_argument("isin", help="ISIN of the instrument")

    hist_parser = subparsers.add_parser("show-history", help="Show time-series data for an instrument")
    hist_parser.add_argument("isin", help="ISIN of the instrument")

    subparsers.add_parser("validate-schema", help="Check if DB schema is up to date with migrations")

    args = parser.parse_args()

    if args.command == "list-instruments":
        list_instruments()
    elif args.command == "show-instrument":
        show_instrument(args.isin)
    elif args.command == "show-history":
        show_history(args.isin)
    elif args.command == "validate-schema":
        validate_schema()
    else:
        parser.print_help()

