import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from tqdm import tqdm

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
}

TARGET_TABLES = {
    "balance_sheet": ["value", "line_item", "period_start", "period_end"],
    "cash_flow": ["value", "line_item", "period_start", "period_end"],
    "earnings": ["value", "line_item", "period_start", "period_end"],
    "financials": ["value", "line_item", "period_start", "period_end"],
    "fundamentals": ["market_cap", "revenue_growth", "profit_margins", "eps_ttm", "return_on_equity"],
    "price_history": ["date"],
    "recommendations": ["period_start", "period_end"],
    "sustainability": ["as_of"]
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def expected_points(start_date, end_date):
    delta = pd.date_range(start=start_date, end=end_date, freq='D')
    return len(set([d.year if d.month >= 4 else d.year - 1 for d in delta]))

def get_all_symbols(conn):
    query = "SELECT symbol FROM tickers"
    return pd.read_sql_query(query, conn)["symbol"].tolist()

def check_completeness(table, fields, start_date, end_date, conn, valid_symbols):
    if table == "price_history":
        query = f"SELECT symbol, date FROM {table} WHERE date >= %s AND date <= %s"
    elif table == "sustainability":
        query = f"SELECT symbol, as_of FROM {table} WHERE as_of >= %s AND as_of <= %s"
    elif table == "recommendations":
        query = f"SELECT symbol, period_start, period_end FROM {table} WHERE period_end >= %s AND period_start <= %s"
    else:
        query = f"SELECT * FROM {table} WHERE period_end >= %s AND period_start <= %s"

    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    if df.empty:
        print(f"[{table}] No data in range.")
        return pd.DataFrame(columns=["symbol", "table", "completeness"])

    df = df[df["symbol"].isin(valid_symbols)]
    if df.empty:
        print(f"[{table}] No data for selected symbols.")
        return pd.DataFrame(columns=["symbol", "table", "completeness"])

    rows = []
    grouped = df.groupby("symbol")
    for symbol, group in grouped:
        if table == "price_history":
            expected = pd.date_range(start=start_date, end=end_date, freq='B').shape[0]
            actual = group["date"].nunique()
        elif table in ["sustainability", "recommendations"]:
            expected = 1
            actual = 1
        elif table == "fundamentals":
            expected = expected_points(start_date, end_date)
            actual = group.dropna(how='all', subset=fields).drop_duplicates(subset=["period_start", "period_end"]).shape[0]
        else:
            expected = expected_points(start_date, end_date)
            actual = group[group[fields].notna().any(axis=1)].drop_duplicates(subset=["period_start", "period_end"]).shape[0]

        completeness = round((actual / expected) * 100, 2) if expected > 0 else 0
        rows.append({"symbol": symbol, "table": table, "completeness": completeness})

    print(f"[{table}] Processed {len(rows)} symbols.")
    return pd.DataFrame(rows)

def generate_completeness_report(start_date, end_date):
    all_results = []
    with get_db_connection() as conn:
        valid_symbols = get_all_symbols(conn)
        print(f"[INFO] Checking completeness for {len(valid_symbols)} symbols.")

        for table, fields in tqdm(TARGET_TABLES.items(), desc="Processing tables"):
            result_df = check_completeness(table, fields, start_date, end_date, conn, valid_symbols)
            if not result_df.empty:
                all_results.append(result_df)

    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        pivoted = combined.pivot(index="symbol", columns="table", values="completeness").reset_index()
        pivoted = pivoted.fillna(0)

        print("\n=== COMPLETENESS REPORT ===")
        print(pivoted.to_string(index=False))

        output_path = f"completeness_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        pivoted.to_csv(output_path, index=False)
        print(f"\n[REPORT] Saved to {output_path}")
    else:
        print("[INFO] No data available for the specified date range.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate completeness report for core tables")
    parser.add_argument("--start_date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", required=True, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    generate_completeness_report(args.start_date, args.end_date)
