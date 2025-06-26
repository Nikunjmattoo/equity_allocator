import os
import json
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# === Setup and Load Environment ===
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_FILE = os.path.join(SCRIPT_DIR, "..", "mapping_files", "variable_mapping.json")
MAPPING_FILE = os.path.normpath(MAPPING_FILE)

# === Load Mapping File ===
def load_mapping():
    try:
        with open(MAPPING_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] Could not load mapping file: {e}")
        return {}

# === Database Setup ===
def get_engine():
    if not DB_URL:
        print("[ERROR] DATABASE_URL not set.")
        exit(1)
    return create_engine(DB_URL)

# === Utility: Get raw input ===
def get_input_value(symbol_data, mapping, key, period_start, period_end):
    sources = mapping.get(key, [])
    for table, field in sources:
        df = symbol_data.get(table)
        if df is not None:
            df_period = df[(df['period_start'] == period_start) & (df['period_end'] == period_end)]
            match = df_period[df_period['line_item'].str.lower() == field.lower()]
            if not match.empty:
                return match.iloc[0]['value']
    return None

# === Compute All Fundamentals ===
def compute_all_fundamentals():
    mapping = load_mapping()
    engine = get_engine()

    summary = []
    all_rows = []
    with engine.begin() as conn:
        tickers = conn.execute(text("SELECT symbol FROM tickers")).fetchall()

    print("[INFO] Starting fundamentals computation...")
    for (symbol,) in tqdm(tickers, desc="Tickers"):
        symbol_data = {}
        for table in ["balance_sheet", "earnings", "cash_flow", "financials"]:
            query = f"SELECT * FROM {table} WHERE symbol = :symbol"
            df = pd.read_sql(text(query), con=engine, params={"symbol": symbol})
            symbol_data[table] = df

        periods = symbol_data['balance_sheet'][['period_start', 'period_end']].drop_duplicates()
        for _, row in periods.iterrows():
            period_start = row['period_start']
            period_end = row['period_end']

            fundamentals = {'symbol': symbol, 'period_start': period_start, 'period_end': period_end}

            ta = get_input_value(symbol_data, mapping, "balance_sheet.total_assets", period_start, period_end)
            tl = get_input_value(symbol_data, mapping, "balance_sheet.total_liabilities", period_start, period_end)
            fundamentals['book_value'] = ta - tl if ta is not None and tl is not None else None

            ebitda = get_input_value(symbol_data, mapping, "financials.ebitda", period_start, period_end)
            rev = get_input_value(symbol_data, mapping, "financials.total_revenue", period_start, period_end)
            fundamentals['ebitda'] = ebitda
            fundamentals['ebitda_margin'] = (ebitda / rev) if ebitda is not None and rev else None

            ni = get_input_value(symbol_data, mapping, "earnings.net_income", period_start, period_end)
            fundamentals['net_income_to_common'] = ni

            shares = get_input_value(symbol_data, mapping, "balance_sheet.shares_outstanding", period_start, period_end)
            fundamentals['revenue_per_share'] = (rev / shares) if rev and shares else None
            fundamentals['total_revenue'] = rev

            gp = get_input_value(symbol_data, mapping, "financials.gross_profit", period_start, period_end)
            fundamentals['gross_margins'] = (gp / rev) if gp and rev else None

            ebit = get_input_value(symbol_data, mapping, "financials.ebit", period_start, period_end)
            fundamentals['operating_margins'] = (ebit / rev) if ebit and rev else None
            fundamentals['profit_margins'] = (ni / rev) if ni and rev else None

            fcf = get_input_value(symbol_data, mapping, "cashflow.free_cashflow", period_start, period_end)
            ocf = get_input_value(symbol_data, mapping, "cashflow.operating_cashflow", period_start, period_end)
            fundamentals['free_cashflow'] = fcf
            fundamentals['operating_cashflow'] = ocf

            eps = get_input_value(symbol_data, mapping, "earnings.diluted_eps", period_start, period_end)
            fundamentals['trailing_eps'] = eps
            fundamentals['eps_ttm'] = eps

            price = get_input_value(symbol_data, mapping, "price_history.close", period_start, period_end)
            fundamentals['price_to_book'] = (price / fundamentals['book_value']) if price and fundamentals['book_value'] else None
            fundamentals['price_to_sales_ttm'] = (price / rev) if price and rev else None

            debt = get_input_value(symbol_data, mapping, "balance_sheet.total_debt", period_start, period_end)
            equity = get_input_value(symbol_data, mapping, "balance_sheet.total_equity", period_start, period_end)
            fundamentals['debt_to_equity'] = (debt / equity) if debt and equity else None
            fundamentals['total_cash'] = get_input_value(symbol_data, mapping, "balance_sheet.total_cash", period_start, period_end)
            fundamentals['total_debt'] = debt
            fundamentals['shares_outstanding'] = shares
            fundamentals['return_on_assets'] = (ni / ta) if ni and ta else None
            fundamentals['return_on_equity'] = (ni / equity) if ni and equity else None

            ca = get_input_value(symbol_data, mapping, "balance_sheet.current_assets", period_start, period_end)
            cl = get_input_value(symbol_data, mapping, "balance_sheet.current_liabilities", period_start, period_end)
            inv = get_input_value(symbol_data, mapping, "balance_sheet.inventory", period_start, period_end)
            fundamentals['current_ratio'] = (ca / cl) if ca and cl else None
            fundamentals['quick_ratio'] = ((ca - inv) / cl) if ca and inv is not None and cl else None
            fundamentals['operating_cashflow_per_share'] = (ocf / shares) if ocf and shares else None
            fundamentals['gross_profit'] = gp
            fundamentals['total_assets'] = ta

            all_rows.append(fundamentals)

            total = len(fundamentals) - 3  # Exclude symbol, period_start, period_end
            filled = sum(1 for k, v in fundamentals.items() if k not in ['symbol', 'period_start', 'period_end'] and v is not None)
            pct = (filled / total) * 100 if total else 0

            print(f"[INFO] Symbol: {symbol}, Period: {period_start} to {period_end}, Filled: {filled}/{total} ({pct:.2f}%)")
            summary.append({
                "symbol": symbol,
                "period_start": str(period_start),
                "period_end": str(period_end),
                "completeness_pct": round(pct, 2)
            })

    # === Insert into DB ===
    if all_rows:
        df_all = pd.DataFrame(all_rows)
        df_all = df_all.where(pd.notnull(df_all), None)
        with engine.begin() as conn:
            df_all.to_sql("fundamentals", conn, if_exists="append", index=False, method="multi")
        print(f"[DB] Inserted {len(df_all)} rows into fundamentals table.")

    print("\n=== Summary of Computation ===")
    df_summary = pd.DataFrame(summary)
    print(df_summary.to_string(index=False))

if __name__ == "__main__":
    compute_all_fundamentals()
