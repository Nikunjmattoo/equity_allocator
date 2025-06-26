#!/usr/bin/env python3
"""
Direct Database Loader - Downloads NSE data and writes directly to PostgreSQL
Bypasses CSV files for faster, more efficient data loading
"""
import os
from dotenv import load_dotenv
import sys
import yfinance as yf
import pandas as pd
import json
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from datetime import datetime, timedelta
import time
import logging

load_dotenv()  # <-- This loads the .env variables

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DirectDatabaseLoader:
    def __init__(self, database_url):
        self.database_url = database_url
        self.conn = None
        self.cur = None
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.cur = self.conn.cursor()
            logger.info("Connected to database")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from database")
    
    def log_operation(self, symbol, data_type, records_processed, records_successful, status, error_msg=None):
        """Log data ingestion operation"""
        try:
            self.cur.execute("""
                INSERT INTO data_ingestion_log 
                (symbol, data_type, records_processed, records_successful, records_failed, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (symbol, data_type, records_processed, records_successful, 
                  records_processed - records_successful, status, error_msg))
        except Exception as e:
            logger.warning(f"Failed to log operation: {e}")
    
    def load_universe(self, symbols):
        """Load stock symbols into universe table"""
        logger.info(f"Loading {len(symbols)} symbols into universe...")
        
        universe_data = [(symbol, f"{symbol} Corporation", "EQ") for symbol in symbols]
        
        try:
            insert_query = """
                INSERT INTO universe (symbol, company_name, series) 
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
            """
            execute_batch(self.cur, insert_query, universe_data, page_size=1000)
            self.conn.commit()
            
            logger.info(f"Successfully loaded {len(universe_data)} symbols into universe")
            self.log_operation("ALL", "universe", len(universe_data), len(universe_data), "SUCCESS")
            
        except Exception as e:
            logger.error(f"Failed to load universe: {e}")
            self.conn.rollback()
            self.log_operation("ALL", "universe", len(universe_data), 0, "FAILED", str(e))
            raise
    
    def download_and_load_price_history(self, symbol, period="10y"):
        """Download price history and load directly to database"""
        logger.info(f"Downloading price history for {symbol}...")
        
        try:
            # Download from yfinance
            ticker = yf.Ticker(f"{symbol}.NS")
            hist = ticker.history(period=period)
            
            if hist.empty:
                logger.warning(f"No price history for {symbol}")
                self.log_operation(symbol, "price_history", 0, 0, "FAILED", "No data available")
                return 0
            
            # Prepare data for database
            price_data = []
            for date, row in hist.iterrows():
                price_data.append((
                    symbol,
                    date.date(),
                    float(row['Open']) if pd.notna(row['Open']) else None,
                    float(row['High']) if pd.notna(row['High']) else None,
                    float(row['Low']) if pd.notna(row['Low']) else None,
                    float(row['Close']) if pd.notna(row['Close']) else None,
                    int(row['Volume']) if pd.notna(row['Volume']) else None
                ))
            
            # Insert into database
            insert_query = """
                INSERT INTO price_history (symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING
            """
            execute_batch(self.cur, insert_query, price_data, page_size=1000)
            self.conn.commit()
            
            logger.info(f"Loaded {len(price_data)} price records for {symbol}")
            self.log_operation(symbol, "price_history", len(price_data), len(price_data), "SUCCESS")
            
            return len(price_data)
            
        except Exception as e:
            logger.error(f"Failed to load price history for {symbol}: {e}")
            self.conn.rollback()
            self.log_operation(symbol, "price_history", 0, 0, "FAILED", str(e))
            return 0
    
    def download_and_load_fundamental_data(self, symbol):
        """Download fundamental data and load directly to database"""
        logger.info(f"Downloading fundamental data for {symbol}...")
        
        try:
            # Download from yfinance
            ticker = yf.Ticker(f"{symbol}.NS")
            info = ticker.info
            
            if not info or len(info) < 5:
                logger.warning(f"No fundamental data for {symbol}")
                self.log_operation(symbol, "fundamental_data", 0, 0, "FAILED", "No data available")
                return 0
            
            # Extract key metrics
            def safe_float(val):
                if val is None or pd.isna(val) or val in ['', 'N/A']:
                    return None
                try:
                    return float(val)
                except:
                    return None
            
            def safe_int(val):
                if val is None or pd.isna(val) or val in ['', 'N/A']:
                    return None
                try:
                    return int(val)
                except:
                    return None
            
            # Prepare fundamental data
            fund_data = (
                symbol,
                datetime.now().date(),
                safe_float(info.get('trailingPE')),
                safe_float(info.get('priceToBook')),
                safe_float(info.get('returnOnEquity')),
                safe_float(info.get('returnOnAssets')),  # Using ROA as proxy for ROCE
                safe_float(info.get('debtToEquity')),
                safe_float(info.get('currentRatio')),
                safe_float(info.get('dividendYield')),
                safe_int(info.get('marketCap')),
                json.dumps(info, default=str)
            )
            
            # Insert into database
            insert_query = """
                INSERT INTO fundamental_data 
                (symbol, data_date, pe_ratio, pb_ratio, roe, roce, debt_to_equity, current_ratio, dividend_yield, market_cap, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, data_date) DO UPDATE SET
                pe_ratio = EXCLUDED.pe_ratio,
                pb_ratio = EXCLUDED.pb_ratio,
                roe = EXCLUDED.roe,
                roce = EXCLUDED.roce,
                debt_to_equity = EXCLUDED.debt_to_equity,
                current_ratio = EXCLUDED.current_ratio,
                dividend_yield = EXCLUDED.dividend_yield,
                market_cap = EXCLUDED.market_cap,
                raw_data = EXCLUDED.raw_data
            """
            
            self.cur.execute(insert_query, fund_data)
            self.conn.commit()
            
            logger.info(f"Loaded fundamental data for {symbol}")
            self.log_operation(symbol, "fundamental_data", 1, 1, "SUCCESS")
            
            return 1
            
        except Exception as e:
            logger.error(f"Failed to load fundamental data for {symbol}: {e}")
            self.conn.rollback()
            self.log_operation(symbol, "fundamental_data", 0, 0, "FAILED", str(e))
            return 0
    
    def download_and_load_financial_statements(self, symbol):
        """Download financial statements and load directly to database"""
        logger.info(f"Downloading financial statements for {symbol}...")
        
        try:
            # Download from yfinance
            ticker = yf.Ticker(f"{symbol}.NS")
            
            total_records = 0
            
            # Balance Sheet
            try:
                balance_sheet = ticker.balance_sheet
                if not balance_sheet.empty:
                    records = self._process_financial_statement(symbol, balance_sheet, "balance_sheet")
                    total_records += records
            except Exception as e:
                logger.warning(f"No balance sheet for {symbol}: {e}")
            
            # Financials (Income Statement)
            try:
                financials = ticker.financials
                if not financials.empty:
                    records = self._process_financial_statement(symbol, financials, "financials")
                    total_records += records
            except Exception as e:
                logger.warning(f"No financials for {symbol}: {e}")
            
            # Cash Flow
            try:
                cashflow = ticker.cashflow
                if not cashflow.empty:
                    records = self._process_financial_statement(symbol, cashflow, "cashflow")
                    total_records += records
            except Exception as e:
                logger.warning(f"No cashflow for {symbol}: {e}")
            
            self.conn.commit()
            
            if total_records > 0:
                logger.info(f"Loaded {total_records} financial statement records for {symbol}")
                self.log_operation(symbol, "financial_statements", total_records, total_records, "SUCCESS")
            else:
                logger.warning(f"No financial statements loaded for {symbol}")
                self.log_operation(symbol, "financial_statements", 0, 0, "FAILED", "No data available")
            
            return total_records
            
        except Exception as e:
            logger.error(f"Failed to load financial statements for {symbol}: {e}")
            self.conn.rollback()
            self.log_operation(symbol, "financial_statements", 0, 0, "FAILED", str(e))
            return 0
    
    def _process_financial_statement(self, symbol, df, statement_type):
        """Process and insert financial statement data"""
        if df.empty:
            return 0
        
        fin_data = []
        for index, row in df.iterrows():
            for col in df.columns:
                value = row[col]
                if pd.notna(value) and value != 0:
                    fin_data.append((
                        symbol,
                        statement_type,
                        col.date() if hasattr(col, 'date') else col,
                        str(index),
                        float(value)
                    ))
        
        if fin_data:
            insert_query = """
                INSERT INTO financial_statements (symbol, statement_type, period_end, line_item, value)
                VALUES (%s, %s, %s, %s, %s)
            """
            execute_batch(self.cur, insert_query, fin_data, page_size=1000)
        
        return len(fin_data)
    
    def update_completeness_tracking(self):
        """Update data completeness tracking table"""
        logger.info("Updating data completeness tracking...")
        
        try:
            # Clear existing completeness data
            self.cur.execute("DELETE FROM data_completeness")
            
            # Calculate and insert completeness data
            self.cur.execute("""
                INSERT INTO data_completeness (
                    symbol, has_price_data, has_fundamental_data, has_financial_statements,
                    price_data_count, fundamental_data_count, financial_statements_count, completeness_percentage
                )
                SELECT 
                    u.symbol,
                    CASE WHEN ph.price_count > 0 THEN TRUE ELSE FALSE END,
                    CASE WHEN fd.fund_count > 0 THEN TRUE ELSE FALSE END,
                    CASE WHEN fs.fin_count > 0 THEN TRUE ELSE FALSE END,
                    COALESCE(ph.price_count, 0),
                    COALESCE(fd.fund_count, 0),
                    COALESCE(fs.fin_count, 0),
                    ROUND(
                        (CASE WHEN ph.price_count > 0 THEN 33.33 ELSE 0 END +
                         CASE WHEN fd.fund_count > 0 THEN 33.33 ELSE 0 END +
                         CASE WHEN fs.fin_count > 0 THEN 33.34 ELSE 0 END), 2
                    )
                FROM universe u
                LEFT JOIN (
                    SELECT symbol, COUNT(*) as price_count 
                    FROM price_history 
                    GROUP BY symbol
                ) ph ON u.symbol = ph.symbol
                LEFT JOIN (
                    SELECT symbol, COUNT(*) as fund_count 
                    FROM fundamental_data 
                    GROUP BY symbol
                ) fd ON u.symbol = fd.symbol
                LEFT JOIN (
                    SELECT symbol, COUNT(*) as fin_count 
                    FROM financial_statements 
                    GROUP BY symbol
                ) fs ON u.symbol = fs.symbol
            """)
            
            self.conn.commit()
            logger.info("Data completeness tracking updated")
            
        except Exception as e:
            logger.error(f"Failed to update completeness tracking: {e}")
            self.conn.rollback()
    
    def load_all_stocks(self, symbols, delay_seconds=1):
        """Load all data for multiple stocks with rate limiting"""
        logger.info(f"Starting direct database loading for {len(symbols)} stocks...")
        
        total_stats = {
            'price_records': 0,
            'fundamental_records': 0,
            'financial_records': 0,
            'successful_stocks': 0,
            'failed_stocks': 0
        }
        
        # Load universe first
        self.load_universe(symbols)
        
        # Process each stock
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"Processing {symbol} ({i}/{len(symbols)})...")
            
            try:
                # Download and load all data types
                price_count = self.download_and_load_price_history(symbol)
                fund_count = self.download_and_load_fundamental_data(symbol)
                fin_count = self.download_and_load_financial_statements(symbol)
                
                # Update stats
                total_stats['price_records'] += price_count
                total_stats['fundamental_records'] += fund_count
                total_stats['financial_records'] += fin_count
                
                if price_count > 0 or fund_count > 0 or fin_count > 0:
                    total_stats['successful_stocks'] += 1
                else:
                    total_stats['failed_stocks'] += 1
                
                # Rate limiting to avoid overwhelming yfinance
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
                
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
                total_stats['failed_stocks'] += 1
        
        # Update completeness tracking
        self.update_completeness_tracking()
        
        # Final summary
        logger.info(f"""
Direct Database Loading Complete:
- Stocks processed: {len(symbols)}
- Successful: {total_stats['successful_stocks']}
- Failed: {total_stats['failed_stocks']}
- Price records: {total_stats['price_records']:,}
- Fundamental records: {total_stats['fundamental_records']:,}
- Financial records: {total_stats['financial_records']:,}
        """)
        
        return total_stats

def main():
    """Main execution function"""
    # Sample stocks for testing
    test_symbols = ['RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'INFY']
    
    # Database connection
    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    # Initialize loader
    loader = DirectDatabaseLoader(database_url)
    
    try:
        # Connect to database
        loader.connect()
        
        # Load all data directly
        stats = loader.load_all_stocks(test_symbols, delay_seconds=1)
        
        print(f"Direct loading completed successfully!")
        print(f"Total records loaded: {stats['price_records'] + stats['fundamental_records'] + stats['financial_records']:,}")
        
    except Exception as e:
        logger.error(f"Direct loading failed: {e}")
        raise
    
    finally:
        # Always disconnect
        loader.disconnect()

if __name__ == "__main__":
    main()