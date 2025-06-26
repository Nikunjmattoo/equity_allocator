#!/usr/bin/env python3
"""
Test script to verify Neon database connection
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def test_connection():
    # Load environment variables
    load_dotenv()
    
    database_url = os.getenv("DATABASE_URL")
    print(f"Testing connection to: {database_url[:50]}...")
    
    try:
        # Create engine
        engine = create_engine(database_url)
        
        # Test connection
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.fetchone()
            print("[SUCCESS] Connection successful!")
            print(f"PostgreSQL version: {version[0]}")
            
            # List tables
            result = connection.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = result.fetchall()
            
            if tables:
                print(f"\n[INFO] Found {len(tables)} tables:")
                for table in tables:
                    print(f"  - {table[0]}")
            else:
                print("\n[INFO] No tables found (database is empty)")
                
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_connection()
