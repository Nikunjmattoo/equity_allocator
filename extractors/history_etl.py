import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
DATA_DIR = os.getenv("DATA_DIR", "data")


def handle_history_files():
    if not os.path.isdir(DATA_DIR):
        print(f"[ERROR] Data directory not found: {DATA_DIR}")
        return

    files = [f for f in os.listdir(DATA_DIR) if f.endswith("_history.csv")]
    if not files:
        print("[INFO] No _history.csv files found.")
        return

    inserted, failed = 0, 0
    errors = []

    with engine.connect() as conn:
        for file in tqdm(files, desc="Processing files"):
            path = os.path.join(DATA_DIR, file)
            try:
                df = pd.read_csv(path)
                if df.empty:
                    continue

                df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date

                # Fill missing optional columns with None
                expected_cols = [
                    "Open", "High", "Low", "Close", "Volume",
                    "Dividends", "Stock Splits", "Capital Gains"
                ]
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = None

                for _, row in tqdm(df.iterrows(), total=len(df), desc=f"{file}", leave=False):
                    if pd.isna(row["Date"]) or pd.isna(row["Close"]) or not row.get("symbol"):
                        continue

                    clean_row = {}
                    for col in ["symbol", "Date"] + expected_cols:
                        val = row.get(col)
                        if pd.isna(val) or val in [float("inf"), float("-inf")]:
                            clean_row[col] = None
                        else:
                            clean_row[col] = val

                    with conn.begin():
                        sql = text("""
                            INSERT INTO price_history (
                                symbol, "Date", "Open", "High", "Low", "Close",
                                "Volume", "Dividends", "Stock Splits", "Capital Gains"
                            )
                            VALUES (
                                :symbol, :Date, :Open, :High, :Low, :Close,
                                :Volume, :Dividends, :Stock_Splits, :Capital_Gains
                            )
                            ON CONFLICT (symbol, "Date") DO UPDATE SET
                                "Open" = EXCLUDED."Open",
                                "High" = EXCLUDED."High",
                                "Low" = EXCLUDED."Low",
                                "Close" = EXCLUDED."Close",
                                "Volume" = EXCLUDED."Volume",
                                "Dividends" = EXCLUDED."Dividends",
                                "Stock Splits" = EXCLUDED."Stock Splits",
                                "Capital Gains" = EXCLUDED."Capital Gains";
                        """)
                        conn.execute(sql, {
                            "symbol": clean_row["symbol"],
                            "Date": clean_row["Date"],
                            "Open": clean_row["Open"],
                            "High": clean_row["High"],
                            "Low": clean_row["Low"],
                            "Close": clean_row["Close"],
                            "Volume": clean_row["Volume"],
                            "Dividends": clean_row["Dividends"],
                            "Stock_Splits": clean_row["Stock Splits"],
                            "Capital_Gains": clean_row["Capital Gains"],
                        })
                        inserted += 1

            except Exception as e:
                failed += 1
                errors.append((file, str(e)))

    print("\n=== Import Report (History) ===")
    print(f"Files processed : {len(files)}")
    print(f"Rows inserted   : {inserted}")
    print(f"Files failed    : {failed}")

    if errors:
        print("\n=== Error Summary (up to 10) ===")
        for file, msg in errors[:10]:
            print(f"[ERROR] {file}: {msg}")
        if len(errors) > 10:
            print(f"...and {len(errors) - 10} more.")


if __name__ == "__main__":
    print("[INFO] Starting historical price import...")
    handle_history_files()
