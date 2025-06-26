import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
DATA_DIR = os.getenv("DATA_DIR", "data")
engine = create_engine(DATABASE_URL)

def load_and_transform_sustainability():
    files = [f for f in os.listdir(DATA_DIR) if f.endswith("_sustainability.csv")]
    if not files:
        print("[INFO] No sustainability files found.")
        return

    records = []
    now = datetime.utcnow()

    for file in tqdm(files, desc="Processing ESG files"):
        try:
            df = pd.read_csv(os.path.join(DATA_DIR, file))
            if df.empty or not {'symbol', 'esg_metric', 'value'}.issubset(df.columns):
                continue

            pivot_df = df.pivot_table(index='symbol', columns='esg_metric', values='value', aggfunc='first')
            pivot_df.columns = [str(col).lower() for col in pivot_df.columns]  # normalize column names
            pivot_df = pivot_df.reset_index()
            pivot_df['as_of'] = now
            pivot_df['as_of'] = now
            records.append(pivot_df)

        except Exception as e:
            print(f"[ERROR] Failed to process {file}: {e}")

    if not records:
        print("[INFO] No ESG data to insert.")
        return

    final_df = pd.concat(records, ignore_index=True)
    final_df = final_df.where(pd.notnull(final_df), None)

    with engine.begin() as conn:
        final_df.to_sql("sustainability", conn, if_exists="append", index=False, method="multi")
        print(f"[DB] Inserted {len(final_df)} ESG records.")

if __name__ == "__main__":
    print("[INFO] Starting ESG import...")
    load_and_transform_sustainability()
