import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# Data folder
DATA_DIR = os.getenv("DATA_DIR", "data")


def is_valid_equity(symbol: str, name: str, sector: str = None, industry: str = None) -> bool:
    if not symbol or not name:
        return False
    symbol = symbol.strip().upper()
    if symbol[:3].isdigit() or "GS" in symbol or "NABAR" in symbol or "IIFCL" in symbol:
        return False
    if len(symbol) > 20 or " " in symbol:
        return False
    if sector is None and industry is None:
        return False
    return True


def clean_json(record: dict) -> str:
    for k, v in record.items():
        if pd.isna(v):
            record[k] = None
        elif isinstance(v, (dict, list)):
            record[k] = json.dumps(v)
        elif not isinstance(v, (str, int, float, type(None))):
            try:
                record[k] = str(v)
            except Exception:
                record[k] = None
    return json.dumps(record, allow_nan=False)


def handle_info_files():
    if not os.path.isdir(DATA_DIR):
        print(f"[ERROR] Data directory not found: {DATA_DIR}")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith("_info.csv")]
    if not files:
        print("[INFO] No _info.csv files found.")
        return

    inserted, skipped, failed = 0, 0, 0
    errors = []

    with engine.connect() as conn:
        for file in files:
            file_path = os.path.join(DATA_DIR, file)
            symbol = None  # fallback in case parsing fails
            try:
                df = pd.read_csv(file_path)
                if df.empty:
                    skipped += 1
                    continue

                row = df.iloc[0].to_dict()
                symbol = str(row.get("symbol") or file.replace("_info.csv", "")).strip().upper()
                name = str(row.get("shortName") or "").strip()
                sector = str(row.get("sector") or "").strip() or None
                industry = str(row.get("industry") or "").strip() or None

                if not is_valid_equity(symbol, name, sector, industry):
                    skipped += 1
                    continue

                full_info = clean_json(row)

                with conn.begin():
                    sql = text("""
                        INSERT INTO tickers (symbol, name, sector, industry, full_info_json)
                        VALUES (:symbol, :name, :sector, :industry, :info_json)
                        ON CONFLICT (symbol) DO UPDATE SET
                            name = EXCLUDED.name,
                            sector = EXCLUDED.sector,
                            industry = EXCLUDED.industry,
                            full_info_json = EXCLUDED.full_info_json;
                    """)
                    conn.execute(sql, {
                        "symbol": symbol,
                        "name": name,
                        "sector": sector,
                        "industry": industry,
                        "info_json": full_info
                    })
                    inserted += 1

            except Exception as e:
                failed += 1
                errors.append((file, symbol or "(unknown)", str(e)))

    print("\n=== Import Report ===")
    print(f"Files processed : {len(files)}")
    print(f"Tickers inserted: {inserted}")
    print(f"Skipped         : {skipped}")
    print(f"Errors          : {failed}")

    if errors:
        print("\n=== Error Summary (showing up to 10) ===")
        for i, (file, symbol, err) in enumerate(errors[:10]):
            print(f"[ERROR] {file} ({symbol}): {err}")
        if len(errors) > 10:
            print(f"...and {len(errors) - 10} more errors.")


if __name__ == "__main__":
    print("[INFO] Starting ticker info import...")
    handle_info_files()
