import pandas as pd
import sqlite3
from database.insert import insert_instrument_data

def load_bberg_excel(file_path):
    df = pd.read_csv(file_path)

    # Rename date column and parse it
    df['Date'] = pd.to_datetime(df['Close of Business Date'], format="%d/%m/%Y").dt.date
    isin = df['ISIN'].iloc[0]

    # Map column name to data_type
    column_map = {
        'Clean Price': 'price_clean',
        'Dirty Price': 'price_dirty',
        'Yield': 'yield',
        'Mod Duration': 'duration_modified',
        'Accrued Interest': 'accrued_interest',
    }

    conn = sqlite3.connect('jcb_db.db')
    inserted, skipped = 0, 0

    for _, row in df.iterrows():
        for col, data_type in column_map.items():
            value = row[col]
            if pd.isna(value) or value == "N/A":
                continue
            try:
                result = insert_instrument_data(
                    conn,
                    instrument_id=isin,
                    data_date=row['Date'],
                    data_type=data_type,
                    value=float(value),
                    source='Bloomberg',
                    resolution='daily'
                )
                if result == "inserted":
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"❌ Error on {row['Date']} ({data_type}): {e}")

    conn.close()
    print(f"✅ Finished: {inserted} inserted, {skipped} skipped.")