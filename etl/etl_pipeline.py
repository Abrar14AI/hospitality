import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from dotenv import load_dotenv
import re

# 1. Load Environment Variables
load_dotenv()

raw_uri = os.getenv("RAW_SUPABASE_DB_URI")
clean_uri = os.getenv("CLEAN_SUPABASE_DB_URI")

if not raw_uri or not clean_uri:
    raise ValueError("Missing URIs in .env. Check RAW_SUPABASE_DB_URI and CLEAN_SUPABASE_DB_URI")

data_path = "data" 

# 2. Table schemas (Standardized for both DBs)
# Note: week_no is TEXT in Raw to accept "W 19", but we clean it later.
tables = {
    'dim_date': """
        CREATE TABLE IF NOT EXISTS dim_date (
            date DATE PRIMARY KEY,
            mmm_yy TEXT,
            week_no TEXT, 
            day_type TEXT
        );
    """,
    'dim_hotels': """
        CREATE TABLE IF NOT EXISTS dim_hotels (
            property_id INTEGER PRIMARY KEY,
            property_name TEXT,
            category TEXT,
            city TEXT
        );
    """,
    'dim_rooms': """
        CREATE TABLE IF NOT EXISTS dim_rooms (
            room_id TEXT PRIMARY KEY,
            room_class TEXT
        );
    """,
    'fact_aggregated_bookings': """
        CREATE TABLE IF NOT EXISTS fact_aggregated_bookings (
            property_id INTEGER,
            check_in_date DATE,
            room_category TEXT,
            successful_bookings INTEGER,
            capacity INTEGER
        );
    """,
    'fact_bookings': """
        CREATE TABLE IF NOT EXISTS fact_bookings (
            booking_id TEXT PRIMARY KEY,
            property_id INTEGER,
            booking_date DATE,
            check_in_date DATE,
            checkout_date DATE,
            no_guests INTEGER,
            room_category TEXT,
            booking_platform TEXT,
            ratings_given FLOAT,
            booking_status TEXT,
            revenue_generated FLOAT,
            revenue_realized FLOAT
        );
    """
}

csv_files = ['dim_date.csv', 'dim_hotels.csv', 'dim_rooms.csv', 'fact_aggregated_bookings.csv', 'fact_bookings.csv']

# ────────────────────────────────────────────────
# HELPER FUNCTIONS

def get_connection(uri):
    return psycopg2.connect(uri)

def get_engine(uri):
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    return create_engine(uri)

def create_tables(uri, schema_dict, label):
    print(f"--- Setting up {label} Database ---")
    conn = get_connection(uri)
    cur = conn.cursor()
    for table, sql in schema_dict.items():
        # FORCED RESET: This kills the "invalid input syntax" ghost
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {label} tables recreated.")

def load_df_to_table(uri, df, table_name):
    # Sanitize column names to match schema
    df.columns = [c.strip().replace(' ', '_').lower() for c in df.columns]
    
    # Handle the specific 'checkout_date' vs 'check_out_date' mismatch
    df = df.rename(columns={'check_out_date': 'checkout_date'})
    
    conn = get_connection(uri)
    cur = conn.cursor()
    columns = [f'"{c}"' for c in df.columns]
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
    
    execute_values(cur, query, df.values.tolist())
    conn.commit()
    cur.close()
    conn.close()
    print(f"   Successfully loaded {len(df)} rows to {table_name}")

def etl_transform(table_name, raw_df):
    df = raw_df.copy()
    
    if table_name == 'dim_date':
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # FIX: Clean "W 19" into "19"
        if 'week_no' in df.columns:
            df['week_no'] = df['week_no'].astype(str).str.replace(r'[^0-9]', '', regex=True)
        df['day_type'] = df['day_type'].fillna('Unknown')

    elif table_name == 'fact_bookings':
        # Clean currency/numbers
        for col in ['ratings_given', 'revenue_generated', 'revenue_realized']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    return df.dropna(how='all')

# ────────────────────────────────────────────────
# MAIN PROCESS

if __name__ == "__main__":
    # STEP 1: Wipe and Recreate both databases
    create_tables(raw_uri, tables, "RAW (Account A)")
    create_tables(clean_uri, tables, "CLEAN (Account B)")

    # STEP 2: Local CSV -> RAW
    print("\n🚀 Step 2: Uploading Local CSVs to RAW Database...")
    for file in csv_files:
        path = os.path.join(data_path, file)
        if os.path.exists(path):
            df = pd.read_csv(path)
            table_name = file.replace('.csv', '')
            load_df_to_table(raw_uri, df, table_name)

    # STEP 3: RAW -> TRANSFORM -> CLEAN
    
    print("\n🚀 Step 3: Transforming Data from RAW to CLEAN...")
    engine_raw = get_engine(raw_uri)
    
    for file in csv_files:
        table_name = file.replace('.csv', '')
        # Read from Account A
        raw_df = pd.read_sql(f'SELECT * FROM "{table_name}"', engine_raw)
        # Clean/Transform
        clean_df = etl_transform(table_name, raw_df)
        # Load to Account B
        load_df_to_table(clean_uri, clean_df, table_name)

    print("\n✨ Mission Accomplished! Data is clean and moved.")