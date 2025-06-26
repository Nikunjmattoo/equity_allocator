"""
Simple comparison between Local and Neon databases
"""
import psycopg2

# Connection strings
LOCAL_DB = "postgresql://postgres:admin@localhost:5432/equity_data"
NEON_DB = "postgresql://equity_allocator_owner:npg_c16DOIXLCqZw@ep-purple-paper-a81d7off-pooler.eastus2.azure.neon.tech/equity_allocator?sslmode=require&channel_binding=require"

def simple_comparison():
    print("SIMPLE DATABASE COMPARISON")
    print("=" * 50)
    
    try:
        # Connect to both
        local_conn = psycopg2.connect(LOCAL_DB)
        neon_conn = psycopg2.connect(NEON_DB)
        
        local_cursor = local_conn.cursor()
        neon_cursor = neon_conn.cursor()
        
        # Get table names from local
        local_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
        tables = [row[0] for row in local_cursor.fetchall()]
        
        print(f"{'Table':<15} {'Local':<10} {'Neon':<10} {'Match?'}")
        print("-" * 45)
        
        total_local = 0
        total_neon = 0
        
        for table in tables:
            # Count local
            local_cursor.execute(f"SELECT COUNT(*) FROM {table};")
            local_count = local_cursor.fetchone()[0]
            total_local += local_count
            
            # Count neon
            neon_cursor.execute(f"SELECT COUNT(*) FROM {table};")
            neon_count = neon_cursor.fetchone()[0]
            total_neon += neon_count
            
            match = "YES" if local_count == neon_count else "NO"
            print(f"{table:<15} {local_count:>8,} {neon_count:>8,} {match:>5}")
        
        print("-" * 45)
        print(f"{'TOTAL':<15} {total_local:>8,} {total_neon:>8,}")
        
        if total_local == total_neon:
            print("\nSUCCESS: All data migrated perfectly!")
        else:
            print(f"\nMISSING: {total_local - total_neon:,} rows need migration")
        
        local_conn.close()  
        neon_conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    simple_comparison()
