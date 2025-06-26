"""
Verify what data actually exists in Neon database
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def verify_neon_connection():
    """Test basic connection and show what actually exists"""
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cursor = conn.cursor()
        
        print("CONNECTED to Neon successfully!")
        print(f"Connection: {os.getenv('DATABASE_URL')[:50]}...")
        
        # List all tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        
        print(f"\nTABLES FOUND: {len(tables)}")
        for table in tables:
            print(f"   - {table[0]}")
        
        if not tables:
            print("NO TABLES FOUND - Database appears empty!")
            return False
        
        # Check row counts for each table
        print(f"\nROW COUNTS:")
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            print(f"   {table_name}: {count:,} rows")
        
        # Show sample data from fundamentals if it exists
        if any('fundamentals' in str(table) for table in tables):
            print(f"\nSAMPLE FROM FUNDAMENTALS:")
            cursor.execute("SELECT symbol, market_cap, total_revenue FROM fundamentals LIMIT 3;")
            samples = cursor.fetchall()
            for sample in samples:
                print(f"   {sample}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"ERROR connecting to Neon: {e}")
        return False

def check_local_vs_neon():
    """Compare local vs Neon database"""
    print("\n" + "="*60)
    print("COMPARING LOCAL VS NEON")
    print("="*60)
    
    # You might have a local connection string too
    local_conn_str = "postgresql://postgres:your_password@localhost:5432/equity_allocator"
    
    print("This would help identify if data exists locally but not in Neon")
    print("We can set up a proper comparison if you have local DB details")

if __name__ == "__main__":
    print("VERIFYING NEON DATABASE CONTENTS")
    print("=" * 50)
    
    success = verify_neon_connection()
    
    if not success:
        print("\nTROUBLESHOOTING STEPS:")
        print("1. Check if DATABASE_URL in .env is correct")
        print("2. Verify you're looking at the right Neon project")
        print("3. Check if data migration actually completed")
        print("4. Consider re-running migration script")
