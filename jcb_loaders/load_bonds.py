import pandas as pd
import sqlite3

from jcb_bond_project.models.instrument import Instrument #Gets the class definition for Instrument from the models file
from jcb_bond_project.models.instrument_data import InstrumentData
from jcb_bond_project.database.insert import save_instrument #this is the function to load the instrument data to the database
from jcb_bond_project.database.insert import insert_instrument_data
from jcb_loaders.classify import classify_bond  #classify is a module that has some ways to filter and clean the spreadsheet information

import re

isin_pattern = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

def is_isin(val):
    return isinstance(val, str) and bool(isin_pattern.match(val.strip()))

def row_bond_to_instrument(row) -> Instrument: #The row of the spreadsheet is transformed into more enriched class Instrument data
    bond_name = row.get('Bond Name')
    isin = row.get('ISIN')

    if pd.isna(bond_name) or pd.isna(isin):
        raise ValueError("Missing bond name or ISIN") #does a quick check to see if the bond name is there

    info = classify_bond(bond_name) #the bond name is used for classifying features this is in the classify.py function

    maturity = pd.to_datetime(row.get('Maturity Date'), errors='coerce')
    issue = pd.to_datetime(row.get('Issue Date'), errors='coerce')

    return Instrument(
        id=isin,
        short_code=info['Short Code'],
        name=bond_name,
        instrument_type='bond',
        issuer='UK Government',
        country='UK',
        currency='GBP',
        maturity_date=maturity.date() if pd.notna(maturity) else None,
        first_issue_date=issue.date() if pd.notna(issue) else None,
        coupon_rate=info['Coupon'],
        is_green=info['Is Green'],
        is_linker=info['Is Linker'],
        index_lag=info['Index Lag'],
        rpi_base=pd.to_numeric(row.get('Base RPI'), errors='coerce')
    )

def row_bond_to_instrument_data(row, data_date) -> list[InstrumentData]:
    """
    Convert a row from the DMO spreadsheet into one or more InstrumentData objects.

    Parameters:
        row: A pandas Series containing the bond data.

    Returns:
        A list of InstrumentData objects.
    """
    isin = row["ISIN"]
    source = "DMO"
    resolution = "irregular"

    data_points = []

    # Amount outstanding
    if not pd.isna(row.get("Amount in Issue")):
        data_points.append(InstrumentData(
            instrument_id=isin,
            data_date=data_date,
            data_type="amount_outstanding",
            value=row["Amount in Issue"],
            source=source,
            resolution=resolution
        ))

    # Index uplifted nominal amount
    if not pd.isna(row.get("Amount in Issue incl Index Uplift")):
        data_points.append(InstrumentData(
            instrument_id=isin,
            data_date=data_date,
            data_type="amount_including_uplift",
            value=row["Amount in Issue incl Index Uplift"],
            source=source,
            resolution=resolution
        ))

    # You can add more fields here similarly if needed

    return data_points

def load_bonds_from_excel(file_path, db_path):
    import re
    from datetime import datetime

    # Read cell A1 to extract the date
    metadata_df = pd.read_excel(file_path, header=None, nrows=1, usecols="A")
    match = re.search(r"Data Date:\s*(\d{1,2}-[A-Za-z]{3}-\d{4})", str(metadata_df.iloc[0, 0]))
    if match:
        data_date = datetime.strptime(match.group(1), "%d-%b-%Y").date()
        print(f"📅 Data Date extracted: {data_date}")
    else:
        raise ValueError("Could not extract data date from cell A1.")

    # Now read the full table starting from row 9 (header=8)
    df = pd.read_excel(file_path, header=8)
    df = df.rename(columns={
        "ISIN Code": "ISIN",
        "Conventional Gilts": "Bond Name",
        "Redemption Date": "Maturity Date",
        "First Issue Date": "Issue Date",
        "Total Amount in Issue \n(£ million nominal)": "Amount in Issue",
        "Unnamed: 7": "Base RPI"
    })

    # Clean the ISINs
    df = df.dropna(subset=["ISIN"])
    df = df[df["ISIN"].apply(is_isin)].copy()

    conn = sqlite3.connect(db_path)

    # Counters
    static_inserted = static_skipped = 0
    dynamic_inserted = dynamic_skipped = 0
    errors = []

    for _, row in df.iterrows():
        try:
            # Static instrument
            inst = row_bond_to_instrument(row)
            result = save_instrument(conn, inst)
            if result == "inserted":
                static_inserted += 1
            else:
                static_skipped += 1

            # Dynamic data
            instrument_data_list = row_bond_to_instrument_data(row, data_date)
            for data_point in instrument_data_list:
                result = insert_instrument_data(conn, data_point)
                if result == "inserted":
                    dynamic_inserted += 1
                else:
                    dynamic_skipped += 1

        except Exception as e:
            errors.append(f"{row.get('ISIN', 'unknown ISIN')}: {e}")

    conn.close()

    # Summary
    print("✅ Static Instrument Data:")
    print(f"   Inserted: {static_inserted}, Skipped: {static_skipped}")
    print("📊 Dynamic Instrument Data:")
    print(f"   Inserted: {dynamic_inserted}, Skipped: {dynamic_skipped}")
    print(f"⚠️ Errors: {len(errors)}")
    for err in errors:
        print(" -", err)

if __name__ == "__main__":
    excel_file = "/Users/jcb/Documents/bond_project/dmo_data/20250529 - Gilts in Issue.xls"
    db_path='jcb_db.db'
    load_bonds_from_excel(excel_file, db_path)