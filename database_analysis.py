"""
Database Analysis Script for Equity Allocator
Analyzes data coverage, fields, tickers, and time periods
"""
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

def get_db_connection():
    """Establish database connection"""
    return psycopg2.connect(os.getenv('DATABASE_URL'))

def analyze_table_structure():
    """Analyze table structures and schemas"""
    conn = get_db_connection()
    
    tables = ['balance_sheet', 'cash_flow', 'earnings', 'financials', 
              'fundamentals', 'price_history', 'recommendations', 
              'sustainability', 'tickers', 'data_contracts']
    
    analysis = {}
    
    try:
        cursor = conn.cursor()
        
        for table in tables:
            print(f"\n{'='*50}")
            print(f"ANALYZING TABLE: {table.upper()}")
            print(f"{'='*50}")
            
            # Get table schema
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = '{table}'
                ORDER BY ordinal_position;
            """)
            columns = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            row_count = cursor.fetchone()[0]
            
            # Get date range if date columns exist
            date_columns = [col[0] for col in columns if 'date' in col[0].lower() or 'time' in col[0].lower()]
            date_range = None
            
            if date_columns:
                date_col = date_columns[0]  # Use first date column
                cursor.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table} WHERE {date_col} IS NOT NULL;")
                date_range = cursor.fetchone()
            
            # Get unique tickers if ticker column exists
            ticker_columns = [col[0] for col in columns if 'ticker' in col[0].lower() or 'symbol' in col[0].lower()]
            unique_tickers = None
            ticker_count = 0
            
            if ticker_columns:
                ticker_col = ticker_columns[0]  # Use first ticker column
                cursor.execute(f"SELECT COUNT(DISTINCT {ticker_col}) FROM {table} WHERE {ticker_col} IS NOT NULL;")
                ticker_count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT DISTINCT {ticker_col} FROM {table} WHERE {ticker_col} IS NOT NULL LIMIT 10;")
                unique_tickers = [row[0] for row in cursor.fetchall()]
            
            analysis[table] = {
                'columns': columns,
                'row_count': row_count,
                'date_range': date_range,
                'date_columns': date_columns,
                'ticker_columns': ticker_columns,
                'ticker_count': ticker_count,
                'unique_tickers': unique_tickers
            }
            
            print(f"Columns ({len(columns)}):")
            for col in columns:
                print(f"  - {col[0]} ({col[1]}) {'NULL' if col[2] == 'YES' else 'NOT NULL'}")
            
            print(f"\nData Summary:")
            print(f"  - Total rows: {row_count:,}")
            print(f"  - Date columns: {date_columns}")
            if date_range:
                print(f"  - Date range: {date_range[0]} to {date_range[1]}")
            print(f"  - Ticker columns: {ticker_columns}")
            print(f"  - Unique tickers: {ticker_count}")
            if unique_tickers:
                print(f"  - Sample tickers: {', '.join(unique_tickers[:10])}")
        
        return analysis
        
    except Exception as e:
        print(f"Error analyzing database: {e}")
        return None
    finally:
        conn.close()

def analyze_data_quality():
    """Analyze data quality and completeness"""
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        
        print(f"\n{'='*60}")
        print("DATA QUALITY ANALYSIS")
        print(f"{'='*60}")
        
        # Check fundamentals data completeness
        print("\n1. FUNDAMENTALS DATA COMPLETENESS:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT CASE WHEN symbol IS NOT NULL THEN symbol END) as unique_tickers,
                MIN(period_start) as earliest_date,
                MAX(period_end) as latest_date
            FROM fundamentals;
        """)
        result = cursor.fetchone()
        if result:
            print(f"   Total records: {result[0]:,}")
            print(f"   Unique tickers: {result[1]:,}")
            print(f"   Date range: {result[2]} to {result[3]}")
        
        # Check price history completeness
        print("\n2. PRICE HISTORY COMPLETENESS:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_tickers,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM price_history;
        """)
        result = cursor.fetchone()
        if result:
            print(f"   Total records: {result[0]:,}")
            print(f"   Unique tickers: {result[1]:,}")
            print(f"   Date range: {result[2]} to {result[3]}")
        
        # Check financial statements coverage
        print("\n3. FINANCIAL STATEMENTS COVERAGE:")
        for table in ['balance_sheet', 'cash_flow', 'earnings']:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT symbol) as unique_tickers,
                    MIN(period_start) as earliest_date,
                    MAX(period_end) as latest_date
                FROM {table};
            """)
            result = cursor.fetchone()
            if result:
                print(f"   {table.upper()}: {result[0]:,} records, {result[1]:,} tickers, {result[2]} to {result[3]}")
        
    except Exception as e:
        print(f"Error in data quality analysis: {e}")
    finally:
        conn.close()

def identify_data_gaps():
    """Identify key data gaps for equity allocation"""
    print(f"\n{'='*60}")
    print("IDENTIFIED DATA GAPS FOR EQUITY ALLOCATION")
    print(f"{'='*60}")
    
    gaps = [
        "MACROECONOMIC DATA",
        "   - Interest rates (Fed funds rate, Treasury yields)",
        "   - Inflation indicators (CPI, PPI)",
        "   - GDP growth rates",
        "   - Employment data",
        "   - Currency exchange rates",
        "",
        "MARKET DATA", 
        "   - Market indices (S&P 500, NASDAQ, etc.)",
        "   - Sector performance metrics",
        "   - Volatility indices (VIX)",
        "   - Options data for risk assessment",
        "",
        "ECONOMIC INDICATORS",
        "   - Consumer confidence index",
        "   - PMI data",
        "   - Credit spreads",
        "   - Commodity prices",
        "",
        "ALTERNATIVE DATA",
        "   - News sentiment analysis",
        "   - Social media sentiment",
        "   - Insider trading data",
        "   - Analyst revisions frequency",
        "",
        "ESG ENHANCED DATA",
        "   - More comprehensive ESG scores",
        "   - Climate risk metrics",
        "   - Governance quality scores"
    ]
    
    for gap in gaps:
        print(gap)

def suggest_data_sources():
    """Suggest data sources to fill gaps"""
    print(f"\n{'='*60}")
    print("RECOMMENDED DATA SOURCES")
    print(f"{'='*60}")
    
    sources = [
        "FREE/OPEN SOURCE:",
        "   - FRED API (Federal Reserve Economic Data) - Macro indicators",
        "   - Yahoo Finance API - Enhanced market data",
        "   - Alpha Vantage - Economic indicators (free tier)",
        "   - Quandl - Economic and financial data",
        "   - News APIs (NewsAPI, Reddit) - Sentiment data",
        "",
        "PAID SERVICES (HIGH VALUE):",
        "   - Bloomberg API - Comprehensive financial data",
        "   - Refinitiv (Thomson Reuters) - Professional grade data",
        "   - FactSet - Institutional quality data",
        "   - S&P Capital IQ - Enhanced fundamentals",
        "   - Morningstar Direct - ESG and sustainability data",
        "",
        "SPECIALIZED APIs:",
        "   - Polygon.io - Real-time market data",
        "   - IEX Cloud - Financial data API",
        "   - Tiingo - End-of-day and intraday data",
        "   - Financial Modeling Prep - Comprehensive financial data"
    ]
    
    for source in sources:
        print(source)

if __name__ == "__main__":
    print("EQUITY ALLOCATOR DATABASE ANALYSIS")
    print("=" * 80)
    
    # Run analysis
    analysis = analyze_table_structure()
    analyze_data_quality()
    identify_data_gaps()
    suggest_data_sources()
    
    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*80}")
