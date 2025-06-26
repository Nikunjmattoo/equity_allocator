"""
Compare Local vs Neon Database - Table by Table Analysis
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

LOCAL_DB_URL = "postgresql://postgres:admin@localhost:5432/equity_data"
NEON_DB_URL = os.getenv('DATABASE_URL')

def compare_databases():
    """Compare table counts between Local and Neon databases"""
    print("DATABASE COMPARISON: LOCAL vs NEON")
    print("="*70)
    
    try:
        # Connect to both databases
        local_conn = psycopg2.connect(LOCAL_DB_URL)
        neon_conn = psycopg2.connect(NEON_DB_URL)
        
        local_cursor = local_conn.cursor()
        neon_cursor = neon_conn.cursor()
        
        # Get all tables from local (source of truth)
        local_cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [table[0] for table in local_cursor.fetchall()]
        
        # Header
        print(f"{'TABLE NAME':<15} | {'LOCAL':<10} | {'NEON':<10} | {'DIFFERENCE':<12} | {'STATUS'}")
        print("-" * 70)
        
        local_total = 0
        neon_total = 0
        migration_status = {}
        
        for table in tables:
            # Get local count
            local_cursor.execute(f"SELECT COUNT(*) FROM {table};")
            local_count = local_cursor.fetchone()[0]
            local_total += local_count
            
            # Get Neon count
            try:
                neon_cursor.execute(f"SELECT COUNT(*) FROM {table};")
                neon_count = neon_cursor.fetchone()[0]
                neon_total += neon_count
            except:
                neon_count = 0
            
            # Calculate difference
            difference = neon_count - local_count
            
            # Determine status
            if local_count == 0:
                status = "EMPTY"
            elif neon_count == local_count:
                status = "PERFECT"
            elif neon_count > local_count:
                status = "EXTRA"
            elif neon_count == 0:
                status = "MISSING"
            else:
                status = "PARTIAL"
            
            migration_status[table] = {
                'local': local_count,
                'neon': neon_count,
                'difference': difference,
                'status': status
            }
            
            # Format difference
            diff_str = f"{difference:+,}" if difference != 0 else "0"
            
            print(f"{table:<15} | {local_count:>8,} | {neon_count:>8,} | {diff_str:>10} | {status}")
        
        # Summary
        print("="*70)
        print(f"{'TOTALS':<15} | {local_total:>8,} | {neon_total:>8,} | {neon_total-local_total:>+10,} |")
        print("="*70)
        
        # Analysis
        print("\nMIGRATION ANALYSIS:")
        print("-" * 30)
        
        perfect_count = sum(1 for t in migration_status.values() if t['status'] == 'PERFECT')
        missing_count = sum(1 for t in migration_status.values() if t['status'] == 'MISSING')
        partial_count = sum(1 for t in migration_status.values() if t['status'] == 'PARTIAL')
        empty_count = sum(1 for t in migration_status.values() if t['status'] == 'EMPTY')
        
        print(f"PERFECT matches: {perfect_count} tables")
        print(f"MISSING data:    {missing_count} tables")
        print(f"PARTIAL data:    {partial_count} tables")
        print(f"EMPTY tables:    {empty_count} tables")
        
        # Overall status
        total_tables_with_data = len(tables) - empty_count
        successful_tables = perfect_count
        
        if successful_tables == total_tables_with_data:
            print(f"\nSUCCESS: 100% migration complete! ({successful_tables}/{total_tables_with_data})")
        else:
            success_rate = (successful_tables / total_tables_with_data) * 100
            print(f"\nPARTIAL: {success_rate:.1f}% migration complete ({successful_tables}/{total_tables_with_data})")
        
        # Issues to address
        issues = []
        for table, info in migration_status.items():
            if info['status'] in ['MISSING', 'PARTIAL']:
                issues.append(f"  - {table}: {info['status']} ({info['local']:,} -> {info['neon']:,})")
        
        if issues:
            print(f"\nISSUES TO ADDRESS:")
            for issue in issues:
                print(issue)
        
        local_conn.close()
        neon_conn.close()
        
    except Exception as e:
        print(f"ERROR during comparison: {e}")

if __name__ == "__main__":
    compare_databases()
