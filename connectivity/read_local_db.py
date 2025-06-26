"""
Read data from Local PostgreSQL Database
"""
import psycopg2

LOCAL_DB_URL = "postgresql://postgres:admin@localhost:5432/equity_data"

def read_local_database():
    """Connect and read from local database"""
    try:
        conn = psycopg2.connect(LOCAL_DB_URL)
        cursor = conn.cursor()
        
        print("LOCAL DATABASE OVERVIEW")
        print("="*50)
        
        # Get all tables and row counts
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        
        total_rows = 0
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            total_rows += count
            print(f"{table_name:<15}: {count:>8,} rows")
        
        print("="*50)
        print(f"TOTAL ROWS: {total_rows:,}")
        
        # Sample data from key tables
        print("\nSAMPLE DATA:")
        print("-" * 30)
        
        # Fundamentals sample
        cursor.execute("SELECT symbol, market_cap, total_revenue FROM fundamentals LIMIT 3;")
        samples = cursor.fetchall()
        print("Fundamentals:")
        for row in samples:
            print(f"  {row[0]}: Market Cap={row[1]}, Revenue={row[2]}")
        
        # Price history sample
        cursor.execute("SELECT symbol, date, close FROM price_history LIMIT 3;")
        samples = cursor.fetchall()
        print("Price History:")
        for row in samples:
            print(f"  {row[0]} on {row[1]}: Close={row[2]}")
        
        conn.close()
        print("\nLOCAL DATABASE READ SUCCESSFUL!")
        
    except Exception as e:
        print(f"ERROR connecting to local database: {e}")

if __name__ == "__main__":
    read_local_database()
