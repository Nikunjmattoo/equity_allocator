"""
Test local database connection before migration
"""
import psycopg2

LOCAL_DB_URL = "postgresql://postgres:admin@localhost:5432/equity_data"

def test_local_connection():
    """Test connection to local database"""
    try:
        print("Testing local database connection...")
        conn = psycopg2.connect(LOCAL_DB_URL)
        cursor = conn.cursor()
        
        print("Connected successfully!")
        
        # List tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        
        print(f"Tables found: {len(tables)}")
        
        # Show row counts
        total_rows = 0
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            total_rows += count
            print(f"  {table_name}: {count:,} rows")
        
        print(f"\nTotal rows in local database: {total_rows:,}")
        conn.close()
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        print("\nTroubleshooting:")
        print("1. Is PostgreSQL running? (Check services)")
        print("2. Is the database name 'equity_data' correct?")
        print("3. Are credentials correct? (postgres/admin)")
        print("4. Is the database on port 5432?")
        return False

if __name__ == "__main__":
    test_local_connection()
