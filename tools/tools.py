# tools/tools.py
"""
Enterprise SQL Tool Engine for AtliQ Hospitality.

Two layers:
1. DETERMINISTIC SQL BUILDER — Python code that generates perfect SQL
2. DIRECT PYTHON FUNCTIONS — Called by the orchestrator (no CrewAI needed)
"""

import pandas as pd
import psycopg2
import re
from crewai.tools import tool
from utils.config import CLEAN_DB_URI, METRIC_LIBRARY


# ════════════════════════════════════════════════════════════════
# SECTION 1: DETERMINISTIC SQL BUILD ENGINE
# ════════════════════════════════════════════════════════════════

_METRIC_CONFIG = {
    'revenue': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "SUM(fb.revenue_realized) AS revenue"
    },
    'total_bookings': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "COUNT(fb.booking_id) AS total_bookings"
    },
    'adr': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "ROUND(SUM(fb.revenue_realized)::numeric / NULLIF(COUNT(fb.booking_id), 0), 2) AS adr"
    },
    'average_rating': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "ROUND(AVG(fb.ratings_given) FILTER(WHERE fb.ratings_given > 0)::numeric, 2) AS average_rating"
    },
    'realisation_pct': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "ROUND((1.0 - (COUNT(fb.booking_id) FILTER(WHERE fb.booking_status IN ('Cancelled','No Show')))::numeric / NULLIF(COUNT(fb.booking_id), 0)) * 100, 2) AS realisation_pct"
    },
    'cancellation_pct': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "ROUND(COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'Cancelled')::numeric / NULLIF(COUNT(fb.booking_id), 0) * 100, 2) AS cancellation_pct"
    },
    'no_show_rate_pct': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "ROUND(COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'No Show')::numeric / NULLIF(COUNT(fb.booking_id), 0) * 100, 2) AS no_show_rate_pct"
    },
    'total_cancelled_bookings': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'Cancelled') AS total_cancelled_bookings"
    },
    'total_checked_out': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'Checked Out') AS total_checked_out"
    },
    'total_no_show': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'No Show') AS total_no_show"
    },
    'occupancy_pct': {
        'type': 'single', 'table': 'fact_aggregated_bookings', 'alias': 'fa',
        'select': "ROUND(SUM(fa.successful_bookings)::numeric / NULLIF(SUM(fa.capacity), 0) * 100, 2) AS occupancy_pct"
    },
    'total_capacity': {
        'type': 'single', 'table': 'fact_aggregated_bookings', 'alias': 'fa',
        'select': "SUM(fa.capacity) AS total_capacity"
    },
    'total_successful_bookings': {
        'type': 'single', 'table': 'fact_aggregated_bookings', 'alias': 'fa',
        'select': "SUM(fa.successful_bookings) AS total_successful_bookings"
    },
    'revpar': {'type': 'cross_table'},
    'dbrn': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "ROUND(COUNT(fb.booking_id)::numeric / NULLIF(COUNT(DISTINCT dd.date), 0), 2) AS dbrn",
        'force_date': True
    },
    'dsrn': {
        'type': 'single', 'table': 'fact_aggregated_bookings', 'alias': 'fa',
        'select': "ROUND(SUM(fa.capacity)::numeric / NULLIF(COUNT(DISTINCT dd.date), 0), 2) AS dsrn",
        'force_date': True
    },
    'durn': {
        'type': 'single', 'table': 'fact_bookings', 'alias': 'fb',
        'select': "ROUND(COUNT(fb.booking_id) FILTER(WHERE fb.booking_status = 'Checked Out')::numeric / NULLIF(COUNT(DISTINCT dd.date), 0), 2) AS durn",
        'force_date': True
    },
    'no_of_days': {
        'type': 'dim_only',
        'select': "COUNT(DISTINCT dd.date) AS no_of_days"
    },
    'booking_pct_by_platform': {
        'type': 'breakdown', 'table': 'fact_bookings', 'alias': 'fb',
        'group_col': 'fb.booking_platform',
        'select': ("fb.booking_platform,\n"
                   "       COUNT(fb.booking_id) AS bookings,\n"
                   "       ROUND(COUNT(fb.booking_id)::numeric / NULLIF(SUM(COUNT(fb.booking_id)) OVER(), 0) * 100, 2) AS booking_pct")
    },
    'booking_pct_by_room_class': {
        'type': 'breakdown', 'table': 'fact_bookings', 'alias': 'fb',
        'group_col': 'dr.room_class',
        'select': ("dr.room_class,\n"
                   "       COUNT(fb.booking_id) AS bookings,\n"
                   "       ROUND(COUNT(fb.booking_id)::numeric / NULLIF(SUM(COUNT(fb.booking_id)) OVER(), 0) * 100, 2) AS booking_pct"),
        'extra_dim': 'dim_rooms'
    }
}

_WOW_MAP = {
    'wow_revenue': 'revenue',
    'wow_occupancy': 'occupancy_pct',
    'wow_adr': 'adr',
    'wow_revpar': 'revpar',
    'wow_realisation': 'realisation_pct',
    'wow_dsrn': 'dsrn',
}

_DIM_JOINS = {
    'dim_hotels': {'alias': 'dh', 'fb': 'fb.property_id = dh.property_id', 'fa': 'fa.property_id = dh.property_id'},
    'dim_date':   {'alias': 'dd', 'fb': 'fb.check_in_date = dd.date',      'fa': 'fa.check_in_date = dd.date'},
    'dim_rooms':  {'alias': 'dr', 'fb': 'fb.room_category = dr.room_id',   'fa': 'fa.room_category = dr.room_id'},
}

_FILTER_MAP = {
    'city':             ('dim_hotels', "dh.city = '{v}'"),
    'property_name':    ('dim_hotels', "dh.property_name = '{v}'"),
    'property_id':      ('dim_hotels', "dh.property_id = {v}"),
    'category':         ('dim_hotels', "dh.category = '{v}'"),
    'week_no':          ('dim_date',   "dd.week_no = '{v}'"),
    'mmm_yy':           ('dim_date',   "dd.mmm_yy = '{v}'"),
    'day_type':         ('dim_date',   "dd.day_type = '{v}'"),
    'room_class':       ('dim_rooms',  "dr.room_class = '{v}'"),
    'booking_platform': (None,         "{fa}.booking_platform = '{v}'"),
    'booking_status':   (None,         "{fa}.booking_status = '{v}'"),
}

_GROUP_MAP = {
    'city':             ('dh.city',             'dim_hotels'),
    'property_name':    ('dh.property_name',    'dim_hotels'),
    'property_id':      ('dh.property_id',      'dim_hotels'),
    'category':         ('dh.category',         'dim_hotels'),
    'week_no':          ('dd.week_no',          'dim_date'),
    'mmm_yy':           ('dd.mmm_yy',          'dim_date'),
    'day_type':         ('dd.day_type',         'dim_date'),
    'room_class':       ('dr.room_class',       'dim_rooms'),
    'booking_platform': ('fb.booking_platform',  None),
}


def _parse_spec(spec_str):
    params = {}
    for part in spec_str.split('|'):
        part = part.strip()
        if '=' in part:
            k, v = part.split('=', 1)
            params[k.strip().lower()] = v.strip()
    return params


def _build_joins_and_wheres(fact_alias, filters, group_cols, force_date=False, extra_dim=None):
    dims = set()
    wheres = []
    group_exprs = []

    for key, val in filters.items():
        if key not in _FILTER_MAP:
            continue
        dim, template = _FILTER_MAP[key]
        if dim:
            dims.add(dim)
            wheres.append(template.format(v=val))
        elif fact_alias == 'fb':
            wheres.append(template.format(fa=fact_alias, v=val))

    for col in group_cols:
        if col in _GROUP_MAP:
            expr, dim = _GROUP_MAP[col]
            if dim:
                dims.add(dim)
            group_exprs.append(expr)

    if force_date:
        dims.add('dim_date')
    if extra_dim:
        dims.add(extra_dim)

    joins = []
    for dim in sorted(dims):
        info = _DIM_JOINS[dim]
        cond = info.get(fact_alias, info['fb'])
        joins.append(f"JOIN {dim} {info['alias']} ON {cond}")

    return joins, wheres, group_exprs


def _assemble_single_query(config, filters, group_cols):
    alias = config['alias']
    table = config['table']
    force_date = config.get('force_date', False)
    extra_dim = config.get('extra_dim')

    joins, wheres, group_exprs = _build_joins_and_wheres(
        alias, filters, group_cols, force_date=force_date, extra_dim=extra_dim
    )

    prefix = ", ".join(group_exprs)
    select_expr = f"{prefix},\n       {config['select']}" if prefix else config['select']

    lines = [f"SELECT {select_expr}", f"FROM {table} {alias}"]
    for j in joins:
        lines.append(f"    {j}")
    if wheres:
        lines.append(f"    WHERE {' AND '.join(wheres)}")
    if group_exprs:
        lines.append(f"    GROUP BY {', '.join(group_exprs)}")
        lines.append(f"    ORDER BY {', '.join(group_exprs)}")

    return "\n".join(lines) + ";"


def _assemble_breakdown_query(config, filters):
    alias = config['alias']
    table = config['table']
    extra_dim = config.get('extra_dim')

    joins, wheres, _ = _build_joins_and_wheres(alias, filters, [], extra_dim=extra_dim)

    lines = [f"SELECT {config['select']}", f"FROM {table} {alias}"]
    for j in joins:
        lines.append(f"    {j}")
    if wheres:
        lines.append(f"    WHERE {' AND '.join(wheres)}")
    lines.append(f"    GROUP BY {config['group_col']}")
    lines.append(f"    ORDER BY booking_pct DESC")

    return "\n".join(lines) + ";"


def _assemble_cross_table_query(filters, group_cols):
    fb_joins, fb_wheres, fb_groups = _build_joins_and_wheres('fb', filters, group_cols)
    fa_joins, fa_wheres, fa_groups = _build_joins_and_wheres('fa', filters, group_cols)

    fb_prefix = ", ".join(fb_groups)
    rev_select = f"{fb_prefix}, SUM(fb.revenue_realized) AS total_revenue" if fb_prefix else "SUM(fb.revenue_realized) AS total_revenue"
    rev_lines = [f"SELECT {rev_select}", "FROM fact_bookings fb"]
    for j in fb_joins:
        rev_lines.append(f"    {j}")
    if fb_wheres:
        rev_lines.append(f"    WHERE {' AND '.join(fb_wheres)}")
    if fb_groups:
        rev_lines.append(f"    GROUP BY {', '.join(fb_groups)}")

    fa_prefix = ", ".join(fa_groups)
    cap_select = f"{fa_prefix}, SUM(fa.capacity) AS total_capacity" if fa_prefix else "SUM(fa.capacity) AS total_capacity"
    cap_lines = [f"SELECT {cap_select}", "FROM fact_aggregated_bookings fa"]
    for j in fa_joins:
        cap_lines.append(f"    {j}")
    if fa_wheres:
        cap_lines.append(f"    WHERE {' AND '.join(fa_wheres)}")
    if fa_groups:
        cap_lines.append(f"    GROUP BY {', '.join(fa_groups)}")

    if fb_groups:
        col_names = [g.split('.')[-1] for g in fb_groups]
        final_cols = ", ".join(f"rev.{c}" for c in col_names)
        final_select = f"{final_cols},\n       ROUND(rev.total_revenue::numeric / NULLIF(cap.total_capacity, 0), 2) AS revpar"
        join_on = " AND ".join(f"rev.{c} = cap.{c}" for c in col_names)
        final_from = f"FROM rev\nJOIN cap ON {join_on}"
    else:
        final_select = "ROUND(rev.total_revenue::numeric / NULLIF(cap.total_capacity, 0), 2) AS revpar"
        final_from = "FROM rev CROSS JOIN cap"

    rev_body = "\n    ".join(rev_lines)
    cap_body = "\n    ".join(cap_lines)

    return f"""WITH rev AS (
    {rev_body}
),
cap AS (
    {cap_body}
)
SELECT {final_select}
{final_from};"""


def _assemble_dim_only_query(config, filters):
    wheres = []
    for key in ('week_no', 'mmm_yy', 'day_type'):
        if key in filters:
            _, template = _FILTER_MAP[key]
            wheres.append(template.format(v=filters[key]))
    where_str = f"\nWHERE {' AND '.join(wheres)}" if wheres else ""
    return f"SELECT {config['select']}\nFROM dim_date dd{where_str};"


def _build_metric_sql(metric_name, filters, group_cols):
    config = _METRIC_CONFIG.get(metric_name)
    if not config:
        return None, f"Unknown metric '{metric_name}'. Available: {list(_METRIC_CONFIG.keys())}"

    mtype = config['type']
    if mtype == 'dim_only':
        return _assemble_dim_only_query(config, filters), None
    elif mtype == 'breakdown':
        return _assemble_breakdown_query(config, filters), None
    elif mtype == 'cross_table':
        return _assemble_cross_table_query(filters, group_cols), None
    elif mtype == 'single':
        return _assemble_single_query(config, filters, group_cols), None
    else:
        return None, f"Unsupported metric type: {mtype}"


def _build_wow_sql(base_metric, current_week, filters):
    prev_week = str(int(current_week) - 1)
    config = _METRIC_CONFIG.get(base_metric)
    if not config:
        return None, f"Unknown base metric for WoW: {base_metric}"

    wow_filters = {k: v for k, v in filters.items() if k != 'week_no'}

    if config['type'] == 'cross_table':
        fb_joins, fb_wheres, _ = _build_joins_and_wheres('fb', wow_filters, [])
        fa_joins, fa_wheres, _ = _build_joins_and_wheres('fa', wow_filters, [])

        if not any('dim_date' in j for j in fb_joins):
            fb_joins.append("JOIN dim_date dd ON fb.check_in_date = dd.date")
        if not any('dim_date' in j for j in fa_joins):
            fa_joins.append("JOIN dim_date dd ON fa.check_in_date = dd.date")

        fb_where = f" AND {' AND '.join(fb_wheres)}" if fb_wheres else ""
        fa_where = f" AND {' AND '.join(fa_wheres)}" if fa_wheres else ""
        fb_join_str = "\n    ".join(fb_joins)
        fa_join_str = "\n    ".join(fa_joins)

        return f"""WITH weekly_rev AS (
    SELECT dd.week_no, SUM(fb.revenue_realized) AS total_revenue
    FROM fact_bookings fb
    {fb_join_str}
    WHERE dd.week_no IN ('{prev_week}', '{current_week}'){fb_where}
    GROUP BY dd.week_no
),
weekly_cap AS (
    SELECT dd.week_no, SUM(fa.capacity) AS total_capacity
    FROM fact_aggregated_bookings fa
    {fa_join_str}
    WHERE dd.week_no IN ('{prev_week}', '{current_week}'){fa_where}
    GROUP BY dd.week_no
),
weekly_metric AS (
    SELECT r.week_no,
           ROUND(r.total_revenue::numeric / NULLIF(c.total_capacity, 0), 2) AS metric_value
    FROM weekly_rev r
    JOIN weekly_cap c ON r.week_no = c.week_no
)
SELECT
    curr.week_no AS current_week,
    curr.metric_value AS current_value,
    prev.metric_value AS previous_value,
    ROUND(((curr.metric_value::numeric / NULLIF(prev.metric_value, 0)) - 1) * 100, 2) AS wow_change_pct
FROM weekly_metric curr
JOIN weekly_metric prev ON curr.week_no::int = prev.week_no::int + 1;""", None

    else:
        alias = config['alias']
        table = config['table']
        metric_select = config['select']
        metric_select_renamed = re.sub(r' AS \w+$', ' AS metric_value', metric_select)

        joins, wheres, _ = _build_joins_and_wheres(alias, wow_filters, [])
        if not any('dim_date' in j for j in joins):
            date_cond = _DIM_JOINS['dim_date'][alias]
            joins.append(f"JOIN dim_date dd ON {date_cond}")

        extra_where = f" AND {' AND '.join(wheres)}" if wheres else ""
        join_str = "\n    ".join(joins)

        return f"""WITH weekly_metric AS (
    SELECT dd.week_no,
           {metric_select_renamed}
    FROM {table} {alias}
    {join_str}
    WHERE dd.week_no IN ('{prev_week}', '{current_week}'){extra_where}
    GROUP BY dd.week_no
)
SELECT
    curr.week_no AS current_week,
    curr.metric_value AS current_value,
    prev.metric_value AS previous_value,
    ROUND(((curr.metric_value::numeric / NULLIF(prev.metric_value, 0)) - 1) * 100, 2) AS wow_change_pct
FROM weekly_metric curr
JOIN weekly_metric prev ON curr.week_no::int = prev.week_no::int + 1;""", None


# ════════════════════════════════════════════════════════════════
# SECTION 2: DIRECT PYTHON FUNCTIONS
# Called by the orchestrator — no CrewAI needed
# ════════════════════════════════════════════════════════════════

def execute_metric_query(metric_name, filters=None, group_by=None):
    """Direct Python function. Returns (DataFrame, sql_string) or (None, error_string)."""
    filters = filters or {}
    group_cols = [g.strip() for g in group_by.split(',') if g.strip()] if group_by else []

    if metric_name in _WOW_MAP:
        base = _WOW_MAP[metric_name]
        current_week = filters.pop('current_week', None) or filters.pop('week_no', None)
        if not current_week:
            return None, f"WoW metrics need current_week. Got filters: {filters}"
        sql, err = _build_wow_sql(base, current_week, filters)
    else:
        sql, err = _build_metric_sql(metric_name, filters, group_cols)

    if err:
        return None, err
    if not sql:
        return None, f"Could not build SQL for '{metric_name}'"

    try:
        conn = psycopg2.connect(CLEAN_DB_URI)
        df = pd.read_sql(sql, conn)
        conn.close()
        return df, sql
    except Exception as e:
        return None, f"SQL Error: {str(e)}\nSQL: {sql}"


def execute_multiple_metrics(metrics_list, filters=None, group_by=None):
    """Execute multiple metrics with same filters/grouping. Returns dict."""
    results = {}
    for metric_name in metrics_list:
        df, sql_or_error = execute_metric_query(metric_name, dict(filters or {}), group_by)
        if df is not None:
            results[metric_name] = {
                "df": df,
                "sql": sql_or_error,
                "error": None,
                "markdown": df.to_markdown(index=False)
            }
        else:
            results[metric_name] = {
                "df": None,
                "sql": None,
                "error": sql_or_error,
                "markdown": None
            }
    return results


def execute_custom_sql(sql_query):
    """Execute arbitrary SQL. Returns (DataFrame, None) or (None, error_string)."""
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

    if not sql_query:
        return None, "Empty query"

    first_kw = sql_query.strip().split()[0].upper()
    if first_kw not in ('SELECT', 'WITH'):
        return None, f"Only SELECT/WITH allowed. Got: {first_kw}"

    sql_upper = sql_query.upper()
    has_fb = bool(re.search(r'\bFACT_BOOKINGS\b', sql_upper))
    has_fa = bool(re.search(r'\bFACT_AGGREGATED_BOOKINGS\b', sql_upper))
    has_cte = sql_upper.strip().startswith('WITH')

    if has_fb and has_fa and not has_cte:
        return None, "BLOCKED: Both fact tables without CTEs. Use execute_metric_query('revpar', ...) instead."

    try:
        conn = psycopg2.connect(CLEAN_DB_URI)
        df = pd.read_sql(sql_query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, f"SQL Error: {str(e)}"


def get_database_context():
    """Direct Python function to get DB context."""
    try:
        conn = psycopg2.connect(CLEAN_DB_URI)

        weeks = pd.read_sql(
            "SELECT week_no, COUNT(*) as days FROM dim_date GROUP BY week_no ORDER BY week_no::int", conn
        )
        cities = pd.read_sql("SELECT DISTINCT city FROM dim_hotels ORDER BY city", conn)['city'].tolist()
        properties = pd.read_sql("SELECT property_id, property_name, category, city FROM dim_hotels ORDER BY property_name", conn)
        platforms = pd.read_sql("SELECT DISTINCT booking_platform FROM fact_bookings ORDER BY booking_platform", conn)['booking_platform'].tolist()
        rooms = pd.read_sql("SELECT room_id, room_class FROM dim_rooms ORDER BY room_id", conn)
        date_range = pd.read_sql("SELECT MIN(date) as start_date, MAX(date) as end_date FROM dim_date", conn).iloc[0]

        conn.close()

        latest_week = weeks.iloc[-1]['week_no']
        full_weeks = weeks[weeks['days'] >= 7]
        latest_full = full_weeks.iloc[-1]['week_no'] if not full_weeks.empty else latest_week

        return {
            "date_range": f"{date_range['start_date']} to {date_range['end_date']}",
            "weeks": weeks['week_no'].tolist(),
            "days_per_week": dict(zip(weeks['week_no'], weeks['days'])),
            "latest_week": latest_week,
            "latest_full_week": latest_full,
            "cities": cities,
            "platforms": platforms,
            "rooms": rooms.to_dict('records'),
            "properties": properties.to_dict('records'),
        }
    except Exception as e:
        return {"error": str(e)}


# ════════════════════════════════════════════════════════════════
# SECTION 3: METRIC ALIAS RESOLVER
# ════════════════════════════════════════════════════════════════

def resolve_metric_name(term):
    """Given natural language term, return matching metric key(s)."""
    term = term.strip().lower()
    matches = []
    for key, info in METRIC_LIBRARY.items():
        all_names = [key.lower()] + [a.lower() for a in info.get('aliases', [])]
        for name in all_names:
            if term in name or name in term:
                matches.append(key)
                break
    return matches


def get_available_metrics():
    return {k: v.get('description', '') for k, v in METRIC_LIBRARY.items()}


def get_available_filters():
    return list(_FILTER_MAP.keys())


def get_available_group_by():
    return list(_GROUP_MAP.keys())