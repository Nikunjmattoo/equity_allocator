import os
import time
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import date, datetime
import json

load_dotenv()

START_DATE = datetime(2015, 1, 1)
END_DATE = datetime.today()

DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
}

TICKERS_FILE = os.path.join(os.getcwd(), 'downloaders', 'tickers.txt')
with open(TICKERS_FILE, 'r') as f:
    TICKERS = [line.strip() for line in f if line.strip()]
DATA_DIR = os.getenv('DATA_DIR', 'data')
LOG_FILE = 'data_loader_errors.log'

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_dataframe(conn, df, table_name, columns=None):
    if df.empty:
        print(f"[SKIP] {table_name}: Empty dataframe")
        return
    with conn.cursor() as cur:
        if columns is None:
            columns = df.columns.tolist()
        values = [tuple(x) for x in df[columns].values]
        cols = ','.join(columns)
        sql = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING"
        execute_values(cur, sql, values)
    conn.commit()
    print(f"[DB] Inserted {len(df)} rows into {table_name}")

def insert_into_universe(conn, ticker):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO universe (symbol, is_active)
            VALUES (%s, true)
            ON CONFLICT (symbol) DO NOTHING
        """, (ticker,))
    conn.commit()

def log_error(ticker, key, error):
    with open(LOG_FILE, 'a') as f:
        f.write(f"[{datetime.now()}] {ticker} - {key}: {str(error)}\n")

def sanitize_value(val):
    if isinstance(val, dict):
        return json.dumps(val)
    return val

def fetch_all_data(ticker):
    print(f"\nFetching {ticker}")
    t = yf.Ticker(ticker)

    try:
        info_df = pd.DataFrame([t.info])
        if 'symbol' not in info_df.columns:
            info_df.insert(0, 'symbol', ticker)
        print(f"[{ticker}] info: OK")
    except Exception as e:
        info_df = pd.DataFrame()
        print(f"[{ticker}] info: ERROR ({e})")

    try:
        history_df = t.history(start=START_DATE, end=END_DATE).reset_index()
        history_df.insert(0, 'symbol', ticker)
        print(f"[{ticker}] history: OK")
    except Exception as e:
        history_df = pd.DataFrame()
        print(f"[{ticker}] history: ERROR ({e})")

    def melt_financial(df, name):
        if df.empty: return pd.DataFrame()
        df = df.reset_index().melt(id_vars='index', var_name='period_end', value_name='value')
        df.rename(columns={'index': 'line_item'}, inplace=True)
        df['statement_type'] = name
        df['symbol'] = ticker
        return df[['symbol', 'statement_type', 'period_end', 'line_item', 'value']]

    financial_df = melt_financial(t.financials, 'income')
    balance_df = melt_financial(t.balance_sheet, 'balance_sheet')
    cashflow_df = melt_financial(t.cashflow, 'cashflow')
    print(f"[{ticker}] financials: {len(financial_df)} | balance_sheet: {len(balance_df)} | cashflow: {len(cashflow_df)}")

    try:
        earnings_df = t.income_stmt.reset_index().melt(id_vars='index', var_name='period', value_name='value')
        earnings_df.rename(columns={'index': 'line_item'}, inplace=True)
        earnings_df['symbol'] = ticker
        print(f"[{ticker}] earnings: OK")
    except:
        earnings_df = pd.DataFrame()
        print(f"[{ticker}] earnings: EMPTY")

    try:
        holders_df = t.institutional_holders.copy(); holders_df['holder_type'] = 'institutional'
        mf_df = t.mutualfund_holders.copy(); mf_df['holder_type'] = 'mutualfund'
        holders_df = pd.concat([holders_df, mf_df])
        holders_df.insert(0, 'symbol', ticker)
        holders_df.rename(columns={
            'Holder': 'holder_name',
            'Shares': 'shares',
            'Date Reported': 'date_reported',
            '% Held': 'percent_held',
            'Value': 'value'
        }, inplace=True)
        print(f"[{ticker}] holders: OK")
    except:
        holders_df = pd.DataFrame()
        print(f"[{ticker}] holders: EMPTY")

    try:
        options_list = []
        for exp in t.options:
            opt = t.option_chain(exp)
            for o, ttype in [(opt.calls, 'call'), (opt.puts, 'put')]:
                o = o.copy(); o['option_type'] = ttype; o['expiration_date'] = exp; o['symbol'] = ticker
                options_list.append(o)
        options_df = pd.concat(options_list) if options_list else pd.DataFrame()
        print(f"[{ticker}] options: OK")
    except:
        options_df = pd.DataFrame()
        print(f"[{ticker}] options: EMPTY")

    try:
        rec_df = t.recommendations.reset_index()
        rec_df.insert(0, 'symbol', ticker)
        if 'index' in rec_df.columns:
            rec_df.drop(columns='index', inplace=True)
        print(f"[{ticker}] recommendations: OK")
    except:
        rec_df = pd.DataFrame()
        print(f"[{ticker}] recommendations: EMPTY")

    try:
        sust_raw = t.sustainability
        print(f"[{ticker}] raw sustainability: {sust_raw}")
        records = []

        if sust_raw is not None and isinstance(sust_raw, dict):
            for key, val in sust_raw.items():
                if isinstance(val, (int, float, str)):
                    records.append({'symbol': ticker, 'esg_metric': key, 'value': sanitize_value(val)})
                elif isinstance(val, dict):
                    for sub_key, sub_val in val.items():
                        metric = f"{key}.{sub_key}"
                        records.append({'symbol': ticker, 'esg_metric': metric, 'value': sanitize_value(sub_val)})
            sust_df = pd.DataFrame(records)
        else:
            fallback = os.path.join(DATA_DIR, f"{ticker}_sustainability.csv")
            if os.path.exists(fallback):
                sust_df = pd.read_csv(fallback)
                print(f"[{ticker}] sustainability: LOADED FROM FILE")
            else:
                sust_df = pd.DataFrame()
                print(f"[{ticker}] sustainability: EMPTY")
    except Exception as e:
        print(f"[{ticker}] sustainability: ERROR ({e})")
        sust_df = pd.DataFrame()

    return {
        'info': info_df,
        'history': history_df,
        'financials': financial_df,
        'balance_sheet': balance_df,
        'cashflow': cashflow_df,
        'earnings': earnings_df,
        'sustainability': sust_df,
        'options': options_df,
        'recommendations': rec_df,
        'holders': holders_df
    }

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = get_db_connection()

    for ticker in TICKERS:
        insert_into_universe(conn, ticker)
        data = fetch_all_data(ticker)

        for key, df in data.items():
            if df.empty:
                print(f"[{ticker}] {key}: DataFrame is empty, skipping DB insert")
                continue

            file_path = f"{DATA_DIR}/{ticker}_{key}.csv"
            df.to_csv(file_path, index=False)
            print(f"[{ticker}] Saved: {file_path}")

            try:
                if key == 'history':
                    price_cols = ['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
                    if 'Adj Close' in df.columns:
                        price_cols.append('Adj Close')
                    insert_dataframe(conn, df, 'price_history', columns=price_cols)

                elif key in ['financials', 'balance_sheet', 'cashflow']:
                    insert_dataframe(conn, df, 'financial_statements')

                elif key == 'earnings':
                    insert_dataframe(conn, df, 'earnings')

                elif key == 'holders':
                    expected = ['symbol', 'holder_name', 'shares', 'date_reported', 'percent_held', 'value', 'holder_type']
                    df = df[[col for col in expected if col in df.columns]]
                    insert_dataframe(conn, df, 'holders')

                elif key == 'options':
                    insert_dataframe(conn, df, 'options_data')

                elif key == 'recommendations':
                    try:
                        # Check if data is in trend format (from CSVs) or analyst format (from yfinance)
                        if {'period', 'strongBuy', 'buy', 'hold', 'sell', 'strongSell'}.issubset(df.columns):
                            df.rename(columns={
                                'strongBuy': 'strong_buy',
                                'strongSell': 'strong_sell'
                            }, inplace=True)
                            valid_cols = ['symbol', 'period', 'strong_buy', 'buy', 'hold', 'sell', 'strong_sell']
                        else:
                            # Skip analyst-level recommendations if schema is not for it
                            print(f"[{ticker}] recommendations: Skipped firm-level (schema mismatch)")
                            continue

                        df = df[[col for col in valid_cols if col in df.columns]]
                        insert_dataframe(conn, df, 'recommendations', columns=valid_cols)
                        print(f"[{ticker}] recommendations: INSERTED")
                    except Exception as e:
                        print(f"[{ticker}] recommendations: INSERT ERROR ({e})")
                        log_error(ticker, key, e)

                elif key == 'sustainability':
                    df['value'] = df['value'].apply(sanitize_value)
                    df['data_date'] = date.today()  # <- ADD THIS LINE
                    insert_dataframe(conn, df, 'sustainability')

                elif key == 'info':
                    df.rename(columns=lambda x: x.lower(), inplace=True)
                    df['data_date'] = date.today()
                    FUNDAMENTAL_COLUMNS = [
                        'symbol', 'marketcap', 'trailingpe', 'pricetobook', 'returnonequity',
                        'returnonassets', 'debttoequity', 'currentratio', 'dividendyield', 'data_date'
                    ]
                    valid_cols = [col for col in FUNDAMENTAL_COLUMNS if col in df.columns]
                    insert_dataframe(conn, df, 'fundamental_data', columns=valid_cols)

            except Exception as e:
                conn.rollback()
                log_error(ticker, key, e)
                print(f"[ERROR] DB insert failed for {ticker} - {key}: {e}")

    conn.close()

if __name__ == '__main__':
    main()
