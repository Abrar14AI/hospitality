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
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {label} tables recreated.")

def load_df_to_table(uri, df, table_name):
    df.columns = [c.strip().replace(' ', '_').lower() for c in df.columns]
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
        
        # Clean week_no: "W 19" → "19"
        if 'week_no' in df.columns:
            df['week_no'] = df['week_no'].astype(str).str.replace(r'[^0-9]', '', regex=True)
        
        # ═══════════════════════════════════════════════════════
        # FIX: Recompute day_type using BUSINESS RULE
        # 
        # Stakeholder/DAX definition:
        #   WEEKDAY(date, 1) → Sun=1, Mon=2, ..., Fri=6, Sat=7
        #   IF(wkd > 5, "Weekend", "Weekday")
        #   So: Friday(6) & Saturday(7) = Weekend
        #       Sunday(1) through Thursday(5) = Weekday
        #
        # Python dt.weekday: Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
        # So: Friday=4, Saturday=5 → Weekend
        #     Everything else → Weekday
        # ═══════════════════════════════════════════════════════
        df['day_type'] = df['date'].dt.weekday.apply(
            lambda x: 'Weekend' if x in (4, 5) else 'Weekday'
        )
        
        # Validation logging
        day_counts = df['day_type'].value_counts()
        print(f"   📊 day_type distribution after fix:")
        for dtype, count in day_counts.items():
            print(f"      {dtype}: {count} days")
        
        # Spot-check Fridays → should be Weekend
        fridays = df[df['date'].dt.weekday == 4][['date', 'day_type']].head(3)
        if not fridays.empty:
            print(f"   ✅ Friday spot-check (should be Weekend):")
            for _, row in fridays.iterrows():
                print(f"      {row['date'].strftime('%Y-%m-%d')} ({row['date'].strftime('%A')}) → {row['day_type']}")
        
        # Spot-check Saturdays → should be Weekend
        saturdays = df[df['date'].dt.weekday == 5][['date', 'day_type']].head(3)
        if not saturdays.empty:
            print(f"   ✅ Saturday spot-check (should be Weekend):")
            for _, row in saturdays.iterrows():
                print(f"      {row['date'].strftime('%Y-%m-%d')} ({row['date'].strftime('%A')}) → {row['day_type']}")
        
        # Spot-check Sundays → should be Weekday
        sundays = df[df['date'].dt.weekday == 6][['date', 'day_type']].head(3)
        if not sundays.empty:
            print(f"   ✅ Sunday spot-check (should be Weekday):")
            for _, row in sundays.iterrows():
                print(f"      {row['date'].strftime('%Y-%m-%d')} ({row['date'].strftime('%A')}) → {row['day_type']}")

    elif table_name == 'fact_bookings':
        for col in ['ratings_given', 'revenue_generated', 'revenue_realized']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    return df.dropna(how='all')


# ────────────────────────────────────────────────
# POST-ETL VERIFICATION
# ────────────────────────────────────────────────

def verify_dim_date(uri):
    """Run after ETL to confirm dim_date is correct."""
    print("\n🔍 POST-ETL VERIFICATION: dim_date")
    print("=" * 50)
    
    conn = get_connection(uri)
    cur = conn.cursor()
    
    # Check 1: day_type values (should be exactly 'Weekend' and 'Weekday')
    cur.execute("SELECT DISTINCT day_type FROM dim_date ORDER BY day_type")
    values = [row[0] for row in cur.fetchall()]
    expected = ['Weekday', 'Weekend']
    status = "✅" if values == expected else "❌"
    print(f"{status} day_type values: {values} (expected: {expected})")
    
    # Check 2: day_type distribution
    cur.execute("""
        SELECT day_type, COUNT(*) as cnt 
        FROM dim_date 
        GROUP BY day_type 
        ORDER BY day_type
    """)
    print("\n   Distribution:")
    for row in cur.fetchall():
        print(f"   {row[0]}: {row[1]} days")
    
    # Check 3: Verify Fridays are Weekend
    cur.execute("""
        SELECT date, day_type, EXTRACT(DOW FROM date) as dow
        FROM dim_date 
        WHERE EXTRACT(DOW FROM date) = 5
        ORDER BY date
        LIMIT 5
    """)
    rows = cur.fetchall()
    all_weekend = all(row[1] == 'Weekend' for row in rows)
    status = "✅" if all_weekend else "❌"
    print(f"\n{status} Fridays marked as Weekend:")
    for row in rows:
        print(f"   {row[0]} → {row[1]} (DOW={int(row[2])})")
    
    # Check 4: Verify Sundays are Weekday
    cur.execute("""
        SELECT date, day_type, EXTRACT(DOW FROM date) as dow
        FROM dim_date 
        WHERE EXTRACT(DOW FROM date) = 0
        ORDER BY date
        LIMIT 5
    """)
    rows = cur.fetchall()
    all_weekday = all(row[1] == 'Weekday' for row in rows)
    status = "✅" if all_weekday else "❌"
    print(f"\n{status} Sundays marked as Weekday:")
    for row in rows:
        print(f"   {row[0]} → {row[1]} (DOW={int(row[2])})")
    
    # Check 5: week_no is clean integer strings
    cur.execute("""
        SELECT week_no FROM dim_date 
        WHERE week_no !~ '^[0-9]+$'
        LIMIT 5
    """)
    dirty = cur.fetchall()
    status = "✅" if not dirty else "❌"
    print(f"\n{status} week_no all clean integers: {'Yes' if not dirty else 'DIRTY VALUES: ' + str(dirty)}")
    
    # Check 6: No old typo values remain
    cur.execute("""
        SELECT COUNT(*) FROM dim_date 
        WHERE day_type IN ('weekeday', 'weekday', 'weekend')
    """)
    old_count = cur.fetchone()[0]
    status = "✅" if old_count == 0 else "❌"
    print(f"\n{status} Old/typo day_type values remaining: {old_count}")
    
    # Check 7: Total row count
    cur.execute("SELECT COUNT(*) FROM dim_date")
    total = cur.fetchone()[0]
    status = "✅" if total == 92 else "⚠️"
    print(f"\n{status} Total rows in dim_date: {total} (expected: 92)")
    
    cur.close()
    conn.close()
    print("=" * 50)


# ────────────────────────────────────────────────
# MAIN PROCESS
# ────────────────────────────────────────────────

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
        raw_df = pd.read_sql(f'SELECT * FROM "{table_name}"', engine_raw)
        clean_df = etl_transform(table_name, raw_df)
        load_df_to_table(clean_uri, clean_df, table_name)

    # STEP 4: Verify the fix
    print("\n✨ Data loaded. Running verification...")
    verify_dim_date(clean_uri)
    
    print("\n✨ Mission Accomplished! Data is clean and verified.")