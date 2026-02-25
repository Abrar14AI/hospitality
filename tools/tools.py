# tools/tools.py
import pandas as pd
import psycopg2
from crewai.tools import tool
from utils.config import CLEAN_DB_URI, METRIC_LIBRARY

# ════════════════════════════════════════════════
# TOOL 1: DATABASE CONTEXT
# Purpose: Time boundaries + dimension values
# ════════════════════════════════════════════════
@tool("get_db_context")
def get_db_context(input_str: str) -> str:
    """Fetches current database metadata: available weeks, cities, date range, 
    and data completeness warnings. Call this FIRST before any analysis."""
    try:
        conn = psycopg2.connect(CLEAN_DB_URI)
        
        # Available weeks with day counts
        weeks_detail = pd.read_sql("""
            SELECT week_no, COUNT(*) as days_in_week 
            FROM dim_date 
            GROUP BY week_no 
            ORDER BY week_no::int
        """, conn)
        
        # Cities
        cities = pd.read_sql(
            "SELECT DISTINCT city FROM dim_hotels ORDER BY city", conn
        )['city'].tolist()
        
        # Date range
        date_range = pd.read_sql(
            "SELECT MIN(date) as start_date, MAX(date) as end_date FROM dim_date", conn
        ).iloc[0]
        
        # Hotel count
        hotel_count = pd.read_sql(
            "SELECT COUNT(DISTINCT property_id) as cnt FROM dim_hotels", conn
        ).iloc[0]['cnt']
        
        conn.close()
        
        # Find incomplete weeks (less than 7 days)
        incomplete = weeks_detail[weeks_detail['days_in_week'] < 7]
        warnings = ""
        if not incomplete.empty:
            for _, row in incomplete.iterrows():
                warnings += f"\n⚠️ Week '{row['week_no']}' has only {row['days_in_week']} day(s) — incomplete week."
        
        latest_week = weeks_detail.iloc[-1]['week_no']
        latest_full_week = weeks_detail[weeks_detail['days_in_week'] >= 7].iloc[-1]['week_no'] if not weeks_detail[weeks_detail['days_in_week'] >= 7].empty else latest_week
        
        return (
            f"DATABASE CONTEXT:\n"
            f"- Date range: {date_range['start_date']} to {date_range['end_date']}\n"
            f"- Available weeks (TEXT type): {weeks_detail['week_no'].tolist()}\n"
            f"- Latest week_no: '{latest_week}'\n"
            f"- Latest FULL week (≥7 days): '{latest_full_week}'\n"
            f"- Valid cities: {cities}\n"
            f"- Total properties: {hotel_count}\n"
            f"- REMINDER: week_no is TEXT. Compare as: week_no = '31'\n"
            f"{warnings}"
        )
    except Exception as e:
        return f"Context Error: {str(e)}"

# ════════════════════════════════════════════════
# TOOL 2: SCHEMA EXPLORER
# Purpose: Dynamic column + type discovery
# ════════════════════════════════════════════════
@tool("explore_schema")
def explore_schema(table_name: str) -> str:
    """Returns column names, data types, and sample values for any table. 
    Use this to verify column names before writing SQL.
    Valid tables: dim_date, dim_hotels, dim_rooms, fact_bookings, fact_aggregated_bookings"""
    table_name = table_name.strip().lower().replace("'", "").replace('"', '')
    
    valid_tables = ['dim_date', 'dim_hotels', 'dim_rooms', 'fact_bookings', 'fact_aggregated_bookings']
    if table_name not in valid_tables:
        return f"Invalid table '{table_name}'. Valid tables: {valid_tables}"
    
    try:
        conn = psycopg2.connect(CLEAN_DB_URI)
        
        # Schema
        schema = pd.read_sql(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position
        """, conn)
        
        # Sample data
        sample = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 3", conn)
        
        # Row count
        count = pd.read_sql(f"SELECT COUNT(*) as total_rows FROM {table_name}", conn).iloc[0]['total_rows']
        
        conn.close()
        
        return (
            f"TABLE: {table_name} ({count:,} rows)\n\n"
            f"COLUMNS:\n{schema.to_markdown(index=False)}\n\n"
            f"SAMPLE DATA:\n{sample.to_markdown(index=False)}"
        )
    except Exception as e:
        return f"Schema Error: {str(e)}"

# ════════════════════════════════════════════════
# TOOL 3: VALUE FINDER
# Purpose: Key-value extraction from actual data
# ════════════════════════════════════════════════
@tool("find_exact_values")
def find_exact_values(search_query: str) -> str:
    """Searches the database for exact values matching a search term.
    Input format: 'field:search_term'
    Supported fields: city, hotel, platform, status, room, category, week, month
    Examples: 'city:Delhi', 'hotel:exotica', 'platform:make', 'status:cancel'
    Or just a plain term to search everywhere: 'Delhi'"""
    try:
        conn = psycopg2.connect(CLEAN_DB_URI)
        
        if ':' in search_query:
            field, term = search_query.split(':', 1)
            field, term = field.strip().lower(), term.strip()
        else:
            field, term = 'all', search_query.strip()
        
        results = []
        
        search_map = {
            'city': ("SELECT DISTINCT city FROM dim_hotels WHERE city ILIKE %s ORDER BY city", 'city'),
            'hotel': ("SELECT property_id, property_name, category, city FROM dim_hotels WHERE property_name ILIKE %s ORDER BY property_name", None),
            'platform': ("SELECT DISTINCT booking_platform FROM fact_bookings WHERE booking_platform ILIKE %s ORDER BY booking_platform", 'booking_platform'),
            'status': ("SELECT DISTINCT booking_status FROM fact_bookings WHERE booking_status ILIKE %s ORDER BY booking_status", 'booking_status'),
            'room': ("SELECT room_id, room_class FROM dim_rooms ORDER BY room_id", None),
            'category': ("SELECT DISTINCT category FROM dim_hotels WHERE category ILIKE %s ORDER BY category", 'category'),
            'week': ("SELECT DISTINCT week_no FROM dim_date ORDER BY week_no::int", None),
            'month': ("SELECT DISTINCT mmm_yy FROM dim_date ORDER BY MIN(date)", None),
        }
        
        fields_to_search = [field] if field != 'all' else list(search_map.keys())
        
        for f in fields_to_search:
            if f not in search_map:
                continue
            query, col = search_map[f]
            
            if '%s' in query:
                df = pd.read_sql(query, conn, params=[f'%{term}%'])
            else:
                df = pd.read_sql(query, conn)
            
            if not df.empty:
                if col:
                    results.append(f"{f.upper()}: {df[col].tolist()}")
                else:
                    results.append(f"{f.upper()}:\n{df.to_markdown(index=False)}")
        
        conn.close()
        return "\n".join(results) if results else f"No matches found for '{term}'"
    except Exception as e:
        return f"Search Error: {str(e)}"

# ════════════════════════════════════════════════
# TOOL 4: SQL EXECUTOR
# Purpose: Run queries with smart error handling
# ════════════════════════════════════════════════
@tool("execute_sql")
def execute_sql(sql_query: str) -> str:
    """Executes a read-only SQL query against the database. 
    Returns results as a markdown table, or a diagnostic error message.
    Only SELECT and WITH (CTE) queries are allowed."""
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
    if not sql_query:
        return "ERROR: Empty query provided."
    
    first_keyword = sql_query.strip().split()[0].upper()
    if first_keyword not in ('SELECT', 'WITH'):
        return f"ERROR: Only SELECT/WITH queries allowed. Got: {first_keyword}"
    
    try:
        conn = psycopg2.connect(CLEAN_DB_URI)
        df = pd.read_sql(sql_query, conn)
        conn.close()
        
        if df.empty:
            return (
                "RESULT: Query executed successfully but returned 0 rows.\n"
                "DIAGNOSTIC CHECKLIST:\n"
                "1. Is week_no compared as TEXT? → week_no = '31' not = 31\n"
                "2. Are string values case-sensitive? → Use ILIKE for safety\n"
                "3. Are filter values verified? → Use find_exact_values tool\n"
                "4. Try a broader query first to confirm data exists"
            )
        
        row_info = f"({len(df)} row{'s' if len(df) != 1 else ''})"
        return f"RESULT {row_info}:\n{df.to_markdown(index=False)}"
    except Exception as e:
        error_msg = str(e)
        diagnostic = "\nDIAGNOSTIC HINTS:"
        
        if "column" in error_msg.lower() and "does not exist" in error_msg.lower():
            diagnostic += "\n- Column name may be wrong. Use explore_schema tool to verify."
        if "syntax error" in error_msg.lower():
            diagnostic += "\n- Check SQL syntax: matching parentheses, commas, quotes."
        if "relation" in error_msg.lower() and "does not exist" in error_msg.lower():
            diagnostic += "\n- Table name may be wrong. Valid: dim_date, dim_hotels, dim_rooms, fact_bookings, fact_aggregated_bookings"
        if "invalid input syntax" in error_msg.lower():
            diagnostic += "\n- Type mismatch. week_no is TEXT, not INT. Use quotes: '31'"
        
        return f"SQL ERROR: {error_msg}{diagnostic}"

# ════════════════════════════════════════════════
# TOOL 5: METRIC LOOKUP
# Purpose: Search the metric library by natural language
# ════════════════════════════════════════════════
@tool("lookup_metric")
def lookup_metric(search_term: str) -> str:
    """Searches the Metric Library for a KPI by name or alias.
    Input: A natural language term like 'occupancy', 'revenue yield', 'conversion rate'
    Returns: The exact SQL formula, required tables, and description."""
    search_term = search_term.strip().lower()
    matches = []
    
    for metric_key, metric_info in METRIC_LIBRARY.items():
        # Check if search term matches key or any alias
        all_names = [metric_key.lower()] + [a.lower() for a in metric_info.get('aliases', [])]
        
        for name in all_names:
            if search_term in name or name in search_term:
                matches.append({
                    'metric': metric_key,
                    'sql': metric_info['sql'],
                    'tables': metric_info['tables'],
                    'description': metric_info['description'],
                    'matched_on': name
                })
                break  # Don't double-match same metric
    
    if not matches:
        # Fallback: show all available metrics
        all_metrics = list(METRIC_LIBRARY.keys())
        return f"No metric found for '{search_term}'. Available metrics: {all_metrics}"
    
    output = f"METRICS MATCHING '{search_term}':\n"
    for m in matches:
        needs_cte = len(m['tables']) > 1 and 'fact_bookings' in m['tables'] and 'fact_aggregated_bookings' in m['tables']
        output += (
            f"\n{'─'*40}\n"
            f"METRIC: {m['metric']}\n"
            f"SQL: {m['sql']}\n"
            f"TABLES: {m['tables']}\n"
            f"DESCRIPTION: {m['description']}\n"
            f"NEEDS CTE: {'YES — fact_bookings and fact_aggregated_bookings cannot be direct-joined' if needs_cte else 'No'}\n"
        )
    
    return output