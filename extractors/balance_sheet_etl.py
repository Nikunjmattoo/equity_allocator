import os
import json
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()
DATA_DIR = os.getenv("DATA_DIR", "data")
DB_URL = os.getenv("DATABASE_URL")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_FILE = os.path.join(SCRIPT_DIR, "..", "mapping_files", "balance_sheet_line_item_mapping.json")
MAPPING_FILE = os.path.normpath(MAPPING_FILE)

def load_mapping(mapping_file):
    try:
        with open(mapping_file, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] Could not load mapping file: {e}")
        return None

def calculate_period_start(period_end):
    try:
        end_date = pd.to_datetime(period_end)
        start_date = end_date - pd.DateOffset(years=1) + timedelta(days=1)
        return start_date.date()
    except Exception:
        return None

def create_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS balance_sheet (
            symbol TEXT,
            line_item TEXT,
            value NUMERIC,
            period_start DATE,
            period_end DATE
        )
        """))

def handle_balance_sheet_files():
    if not os.path.isdir(DATA_DIR):
        print(f"[ERROR] Data directory not found: {DATA_DIR}")
        return

    mapping = load_mapping(MAPPING_FILE)
    if mapping is None:
        return

    reverse_map = set(mapping.keys())
    files = [f for f in os.listdir(DATA_DIR) if f.endswith("_balance_sheet.csv")]

    unmapped_items = set()
    total_rows = 0
    inserted_rows = 0
    skipped_rows = 0
    skipped_samples = []
    failed_files = []

    if not DB_URL:
        print("[ERROR] DATABASE_URL not set in environment.")
        return

    engine = create_engine(DB_URL)
    create_table(engine)

    print("[INFO] Starting balance sheet import...")
    for file in tqdm(files, desc="Processing files"):
        path = os.path.join(DATA_DIR, file)
        try:
            df = pd.read_csv(path)
            symbol = file.replace("_balance_sheet.csv", "")

            if df.empty:
                continue

            valid_rows = []
            for _, row in tqdm(df.iterrows(), total=len(df), desc=f"{file}", leave=False):
                line_item = str(row.get("line_item", "")).strip()
                value = row.get("value", None)
                period_end = row.get("period_end", None)
                period_start = calculate_period_start(period_end)

                skip_reason = []
                if not line_item:
                    skip_reason.append("missing line_item")
                if pd.isna(value):
                    skip_reason.append("missing value")
                if pd.isna(period_end):
                    skip_reason.append("missing period_ending")
                if not period_start:
                    skip_reason.append("invalid period_start")

                if skip_reason:
                    skipped_rows += 1
                    if len(skipped_samples) < 10:
                        skipped_samples.append({
                            "file": file,
                            "line_item": line_item,
                            "value": value,
                            "period_ending": period_end,
                            "reason": ", ".join(skip_reason)
                        })
                    continue

                if line_item not in reverse_map:
                    unmapped_items.add(line_item)

                valid_rows.append({
                    "symbol": symbol,
                    "line_item": line_item,
                    "value": value,
                    "period_start": period_start,
                    "period_end": pd.to_datetime(period_end).date()
                })

                total_rows += 1

            if valid_rows:
                try:
                    pd.DataFrame(valid_rows).to_sql("balance_sheet", engine, if_exists="append", index=False)
                    inserted_rows += len(valid_rows)
                except Exception as e:
                    print(f"[ERROR] DB insertion failed for file {file}: {e}")

        except Exception as e:
            failed_files.append((file, str(e)))

    print("\n=== Import Report (Balance Sheet) ===")
    print(f"Files processed : {len(files)}")
    print(f"Rows checked    : {total_rows + skipped_rows}")
    print(f"Rows skipped    : {skipped_rows}")
    print(f"Rows inserted   : {inserted_rows}")
    print(f"Files failed    : {len(failed_files)}")

    if skipped_samples:
        print("\n=== Skipped Row Samples (up to 10) ===")
        for sample in skipped_samples:
            print(sample)

    if failed_files:
        print("\n=== Error Summary (up to 10) ===")
        for file, msg in failed_files[:10]:
            print(f"[ERROR] {file}: {msg}")
        if len(failed_files) > 10:
            print(f"...and {len(failed_files) - 10} more.")

    if unmapped_items:
        print("\n=== Unmapped line_item values ===")
        for item in sorted(unmapped_items):
            print(f"- {item}")
        print(f"\n[WARNING] {len(unmapped_items)} unmapped line items found. Please update the mapping before continuing.")
    else:
        print("\n[INFO] All line_item values successfully mapped. Ready for DB ingestion.")

if __name__ == "__main__":
    handle_balance_sheet_files()
