# yfinance_full_downloader.py
"""
Maximal Data Downloader using yfinance
Fetches and structures nearly all available fields from Yahoo Finance for each ticker.
"""

import yfinance as yf
import pandas as pd
import os

def fetch_all_data(ticker):
    t = yf.Ticker(ticker)

    # 1. info
    info_df = pd.DataFrame([t.info])
    info_df.insert(0, 'ticker', ticker)

    # 2. history
    history_df = t.history(period="max").reset_index()
    history_df.insert(0, 'ticker', ticker)

    # 3. financials
    def melt_financial(df, name):
        if df.empty:
            return pd.DataFrame()
        df = df.reset_index()
        df_melt = df.melt(id_vars='index', var_name='period_end', value_name='value')
        df_melt.rename(columns={'index': 'line_item'}, inplace=True)
        df_melt['statement_type'] = name
        df_melt['ticker'] = ticker
        return df_melt[['ticker', 'statement_type', 'period_end', 'line_item', 'value']]

    financials_df = melt_financial(t.financials, 'income')
    balance_df = melt_financial(t.balance_sheet, 'balance_sheet')
    cashflow_df = melt_financial(t.cashflow, 'cashflow')

    # 4. earnings (from income_stmt)
    try:
        earnings_df = t.income_stmt.reset_index().melt(id_vars='index', var_name='period', value_name='value')
        earnings_df.rename(columns={'index': 'line_item'}, inplace=True)
        earnings_df['ticker'] = ticker
    except Exception:
        earnings_df = pd.DataFrame()

    # 5. sustainability
    try:
        sustainability_df = t.sustainability.reset_index()
        sustainability_df.insert(0, 'ticker', ticker)
    except Exception:
        sustainability_df = pd.DataFrame()

    # 6. calendar
    try:
        calendar_dict = t.calendar.to_dict()
        calendar_df = pd.DataFrame([calendar_dict])
        calendar_df.insert(0, 'ticker', ticker)
    except Exception:
        calendar_df = pd.DataFrame()

    # 7. options
    options_list = []
    try:
        for exp in t.options:
            opt_chain = t.option_chain(exp)
            for opt_df, opt_type in [(opt_chain.calls, 'call'), (opt_chain.puts, 'put')]:
                opt_df = opt_df.copy()
                opt_df['option_type'] = opt_type
                opt_df['expiration_date'] = exp
                opt_df['ticker'] = ticker
                options_list.append(opt_df)
        options_df = pd.concat(options_list) if options_list else pd.DataFrame()
    except Exception:
        options_df = pd.DataFrame()

    # 8. recommendations
    try:
        recommendations_df = t.recommendations.reset_index()
        recommendations_df.insert(0, 'ticker', ticker)
    except Exception:
        recommendations_df = pd.DataFrame()

    # 9. holders
    try:
        institutional_df = t.institutional_holders.copy()
        institutional_df['holder_type'] = 'institutional'
        mutualfund_df = t.mutualfund_holders.copy()
        mutualfund_df['holder_type'] = 'mutualfund'
        holders_df = pd.concat([institutional_df, mutualfund_df])
        holders_df.insert(0, 'ticker', ticker)
    except Exception:
        holders_df = pd.DataFrame()

    return {
        'info': info_df,
        'history': history_df,
        'financials': financials_df,
        'balance_sheet': balance_df,
        'cashflow': cashflow_df,
        'earnings': earnings_df,
        'sustainability': sustainability_df,
        'calendar': calendar_df,
        'options': options_df,
        'recommendations': recommendations_df,
        'holders': holders_df
    }

def main():
    tickers = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"]
    os.makedirs("data", exist_ok=True)

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        data = fetch_all_data(ticker)
        for key, df in data.items():
            if not df.empty:
                filename = f"data/{ticker}_{key}.csv"
                df.to_csv(filename, index=False)
                print(f"Saved {filename}")

if __name__ == "__main__":
    main()