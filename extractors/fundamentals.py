import os
import glob
import pandas as pd
from datetime import date
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
}

DATA_DIR = os.getenv('DATA_DIR', 'data')
FIELDS = {
    'marketcap': 'market_cap',
    'enterprisevalue': 'enterprise_value',
    'bookvalue': 'book_value',
    'ebitda': 'ebitda',
    'ebitdamargins': 'ebitda_margin',
    'netincometocommon': 'net_income_to_common',
    'revenuepershare': 'revenue_per_share',
    'totalrevenue': 'total_revenue',
    'grossmargins': 'gross_margins',
    'operatingmargins': 'operating_margins',
    'profitmargins': 'profit_margins',
    'freecashflow': 'free_cashflow',
    'operatingcashflow': 'operating_cashflow',
    'epstrailingtwelvemonths': 'eps_ttm',
    'trailingeps': 'trailing_eps',
    'trailingpe': 'trailing_pe',
    'forwardpe': 'forward_pe',
    'pricetobook': 'price_to_book',
    'pricetosalestrailing12months': 'price_to_sales_ttm',
    'debttoequity': 'debt_to_equity',
    'totalcash': 'total_cash',
    'totaldebt': 'total_debt',
    'sharesoutstanding': 'shares_outstanding',
    'returnonassets': 'return_on_assets',
    'returnonequity': 'return_on_equity',
    'payoutratio': 'payout_ratio',
    'earningsgrowth': 'earnings_growth',
    'revenuegrowth': 'revenue_growth',
    'dividendyield': 'dividend_yield',
    'dividendrate': 'dividend_rate',
    'fiveyearavgdividendyield': 'dividend_yield_5y_avg',
    'currentratio': 'current_ratio',
    'quickratio': 'quick_ratio',
    'pegratio': 'peg_ratio',
    'beta': 'beta',
    'operatingcashflowpershare': 'operating_cashflow_per_share',
    'grossprofit': 'gross_profit',
    'currentprice': 'current_price',
    'totalassets': 'total_assets'
}


def get_current_fy():
    today = date.today()
    year = today.year
    if today.month >= 4:
        return date(year, 4, 1), date(year + 1, 3, 31)
    else:
        return date(year - 1, 4, 1), date(year, 3, 31)


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_valid_tickers():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM tickers")
            return {row[0] for row in cur.fetchall()}


def clean_value(val):
    if pd.isna(val):
        return None
    if isinstance(val, str):
        val = val.strip().replace(',', '').lower()
        if val in {"na", "n/a", "nan", "null", "", "none"}:
            return None
    try:
        return float(val)
    except:
        return None


def insert_dataframe(conn, df, table_name):
    if df.empty:
        print("[SKIP] No data to insert.")
        return

    # Replace all np.nan with None explicitly
    df = df.astype(object).where(pd.notnull(df), None)

    # Drop rows where all non-symbol/period fields are None
    non_keys = [col for col in df.columns if col not in {'symbol', 'period_start', 'period_end'}]
    df = df.dropna(subset=non_keys, how='all')

    with conn.cursor() as cur:
        cols = df.columns.tolist()
        values = [tuple(row) for row in df.to_numpy()]
        sql = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES %s ON CONFLICT (symbol, period_end) DO NOTHING"
        execute_values(cur, sql, values)
    conn.commit()
    print(f"[DB] Inserted {len(df)} rows into {table_name}")


def main():
    period_start, period_end = get_current_fy()
    print(f"[INFO] Current financial period: {period_start} to {period_end}")

    valid_tickers = get_valid_tickers()
    records = []

    for symbol in tqdm(valid_tickers, desc="Processing info files"):
        file_path = os.path.join(DATA_DIR, f"{symbol}_info.csv")
        if not os.path.isfile(file_path):
            continue

        try:
            df = pd.read_csv(file_path)
            if df.empty:
                continue
            df.columns = df.columns.str.lower()

            rec = {'symbol': symbol, 'period_start': period_start, 'period_end': period_end}
            for raw_col, clean_col in FIELDS.items():
                if raw_col in df.columns:
                    rec[clean_col] = clean_value(df.at[0, raw_col])
            records.append(rec)
        except Exception as ex:
            print(f"[ERROR] {symbol}: {ex}")

    final_df = pd.DataFrame(records)
    if not final_df.empty:
        with get_db_connection() as conn:
            insert_dataframe(conn, final_df, "fundamentals")


if __name__ == '__main__':
    main()
