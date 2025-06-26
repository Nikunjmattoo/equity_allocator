import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from tqdm import tqdm
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "data")
DB_URL = os.getenv("DATABASE_URL")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def calculate_period_bounds(relative_period):
    try:
        today = datetime.today()
        offset_months = int(relative_period.replace("m", ""))
        period_end = (today + relativedelta(months=offset_months)).replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
        period_start = period_end.replace(day=1)
        return period_start.date(), period_end.date()
    except Exception:
        return None, None

def create_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS recommendations (
                symbol TEXT,
                period_start DATE,
                period_end DATE,
                strongBuy NUMERIC,
                buy NUMERIC,
                hold NUMERIC,
                sell NUMERIC,
                strongSell NUMERIC
            )
        """))

def handle_recommendations_files():
    if not os.path.isdir(DATA_DIR):
        print(f"[ERROR] Data directory not found: {DATA_DIR}")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith("_recommendations.csv")]
    skipped_samples = []
    failed_files = []
    total_rows, skipped_rows = 0, 0
    insert_rows = []

    if not DB_URL:
        print("[ERROR] DATABASE_URL not set.")
        return

    engine = create_engine(DB_URL)
    create_table(engine)

    print("[INFO] Starting recommendations import...")
    for file in tqdm(files, desc="Processing files"):
        path = os.path.join(DATA_DIR, file)
        try:
            df = pd.read_csv(path)
            symbol = file.replace("_recommendations.csv", "")

            if df.empty:
                continue

            for _, row in tqdm(df.iterrows(), total=len(df), desc=file, leave=False):
                period_raw = str(row.get("period", "")).strip()
                period_start, period_end = calculate_period_bounds(period_raw)

                total_rows += 1
                if not period_start or not period_end:
                    skipped_rows += 1
                    if len(skipped_samples) < 10:
                        skipped_samples.append({
                            "file": file,
                            "period": period_raw,
                            "reason": "invalid or missing period"
                        })
                    continue

                insert_rows.append({
                "symbol": symbol,
                "period_start": period_start,
                "period_end": period_end,
                "strongbuy": row.get("strongBuy", None),
                "buy": row.get("buy", None),
                "hold": row.get("hold", None),
                "sell": row.get("sell", None),
                "strongsell": row.get("strongSell", None)
            })

        except Exception as e:
            failed_files.append((file, str(e)))

    # Insert to DB
    if insert_rows:
        try:
            pd.DataFrame(insert_rows).to_sql("recommendations", engine, if_exists="append", index=False)
        except Exception as e:
            print(f"[ERROR] DB insertion failed: {e}")

    print("\n=== Import Report (Recommendations) ===")
    print(f"Files processed : {len(files)}")
    print(f"Rows checked    : {total_rows}")
    print(f"Rows skipped    : {skipped_rows}")
    print(f"Rows inserted   : {len(insert_rows)}")
    print(f"Files failed    : {len(failed_files)}")

    if skipped_samples:
        print("\n=== Skipped Row Samples (up to 10) ===")
        for s in skipped_samples:
            print(s)

    if failed_files:
        print("\n=== Failed Files ===")
        for file, err in failed_files:
            print(f"[ERROR] {file}: {err}")

if __name__ == "__main__":
    handle_recommendations_files()
