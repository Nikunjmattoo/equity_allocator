"""
Check what tables exist in Neon
"""
import psycopg2

NEON_DB = "postgresql://equity_allocator_owner:npg_c16DOIXLCqZw@ep-purple-paper-a81d7off-pooler.eastus2.azure.neon.tech/equity_allocator?sslmode=require&channel_binding=require"

def check_tables():
    try:
        conn = psycopg2.connect(NEON_DB)
        cursor = conn.cursor()
        
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
        tables = cursor.fetchall()
        
        print("NEON DATABASE TABLES:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]};")
            count = cursor.fetchone()[0]
            print(f"{table[0]}: {count:,} rows")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_tables()
