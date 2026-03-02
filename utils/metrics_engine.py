# utils/metrics_engine.py
"""
Shared Metrics Engine for AtliQ Hospitality.

Single source of truth for all KPI calculations across:
  - Executive Dashboard (dashboard.py)
  - KPI Monitoring & Anomaly Detection (03_KPI_Monitoring.py)
  - Chat Agent (agents.py — via tools/tools.py directly)

Wraps the deterministic SQL builder in tools/tools.py.
Same formulas → same SQL → same numbers everywhere.
"""

import pandas as pd
from tools.tools import (
    execute_metric_query,
    execute_custom_sql,
    get_database_context,
)


# ════════════════════════════════════════════════
# CONSTANTS
# ════════════════════════════════════════════════

CORE_METRICS = [
    "revenue", "total_bookings", "adr", "average_rating",
    "realisation_pct", "cancellation_pct", "no_show_rate_pct",
    "occupancy_pct", "total_capacity", "total_successful_bookings",
    "revpar", "dbrn", "dsrn", "durn", "no_of_days",
    "total_cancelled_bookings", "total_checked_out", "total_no_show",
]

_WOW_KEYS = {
    "revenue":     "wow_revenue",
    "occupancy":   "wow_occupancy",
    "adr":         "wow_adr",
    "revpar":      "wow_revpar",
    "realisation": "wow_realisation",
    "dsrn":        "wow_dsrn",
}


# ════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════

def _safe_scalar(df):
    """Extract single numeric value from a 1-row metric result."""
    if df is None or df.empty:
        return 0
    val = df.iloc[0, -1]
    try:
        return float(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0


def get_latest_full_week():
    """Return the latest week_no that has a full 7 days of data."""
    ctx = get_database_context()
    if "error" in ctx:
        return None
    return ctx.get("latest_full_week")


def get_db_context():
    """Expose full database context (weeks, cities, platforms, etc.)."""
    return get_database_context()


def build_dashboard_filters(
    city="All", category="All", room_class="All",
    month=None, week=None
):
    """
    Convert Streamlit UI selections into a filters dict
    compatible with all metric functions.

    Usage:
        filters = build_dashboard_filters(
            city=selected_city,
            category=selected_category,
            week=selected_week
        )
        metrics = get_core_metrics(filters)
    """
    f = {}
    if city and city != "All":
        f["city"] = city
    if category and category != "All":
        f["category"] = category
    if room_class and room_class != "All":
        f["room_class"] = room_class
    if month and month != "All":
        f["mmm_yy"] = month
    if week and str(week) != "All":
        f["week_no"] = str(week)
    return f


# ════════════════════════════════════════════════
# CORE METRICS (scalar KPI values)
# ════════════════════════════════════════════════

def get_core_metrics(filters=None):
    """
    Calculate all core KPIs with optional filters.

    Returns:
        dict: {'revenue': 1709542170, 'occupancy_pct': 57.87, 'adr': 12696.35, ...}

    Example:
        m = get_core_metrics({'city': 'Delhi'})
        print(f"Revenue: ₹{m['revenue']/1e6:.1f}M")
        print(f"Occupancy: {m['occupancy_pct']:.1f}%")
    """
    filters = filters or {}
    result = {}
    for metric in CORE_METRICS:
        df, _ = execute_metric_query(metric, dict(filters))
        result[metric] = _safe_scalar(df)
    return result


# ════════════════════════════════════════════════
# WEEK-OVER-WEEK DELTAS
# ════════════════════════════════════════════════

def get_wow_delta(metric_key, current_week, filters=None):
    """
    Calculate WoW change for one metric.

    Args:
        metric_key: 'revenue', 'occupancy', 'adr', 'revpar', 'realisation', 'dsrn'
        current_week: week number as string, e.g. '31'
        filters: optional additional filters (city, category, etc.)

    Returns:
        Formatted string: '+5.2%', '-3.1%', or 'N/A'
    """
    wow_name = _WOW_KEYS.get(metric_key)
    if not wow_name:
        return "N/A"

    f = dict(filters or {})
    f["current_week"] = str(current_week)

    df, _ = execute_metric_query(wow_name, f)
    if df is not None and not df.empty and "wow_change_pct" in df.columns:
        val = df.iloc[0]["wow_change_pct"]
        if val is not None:
            val = float(val)
            sign = "+" if val >= 0 else ""
            return f"{sign}{val:.1f}%"
    return "N/A"


def get_all_wow_deltas(current_week, filters=None):
    """
    All 6 WoW deltas at once.

    Returns:
        dict: {'revenue': '+5.2%', 'occupancy': '-1.3%', 'adr': '+2.0%', ...}
    """
    return {key: get_wow_delta(key, current_week, filters) for key in _WOW_KEYS}


# ════════════════════════════════════════════════
# GROUPED / DIMENSIONAL DATA
# ════════════════════════════════════════════════

def get_metric_by_dimension(metric_name, group_by, filters=None):
    """
    Single metric grouped by a dimension.

    Args:
        metric_name: any metric from CORE_METRICS
        group_by: 'city', 'property_name', 'category', 'week_no',
                  'day_type', 'room_class', 'booking_platform', 'mmm_yy'
        filters: optional filters dict

    Returns:
        DataFrame with [group_column, metric_column]

    Example:
        df = get_metric_by_dimension('revenue', 'city')
        # city     | revenue
        # Delhi    | 295M
        # Mumbai   | 669M
    """
    df, _ = execute_metric_query(metric_name, dict(filters or {}), group_by=group_by)
    return df if df is not None else pd.DataFrame()


def get_multi_metric_by_dimension(metrics, group_by, filters=None):
    """
    Multiple metrics grouped by the same dimension, merged into one DataFrame.

    Args:
        metrics: list of metric names
        group_by: dimension column name
        filters: optional filters dict

    Returns:
        DataFrame with [group_column, metric1, metric2, ...]

    Example:
        df = get_multi_metric_by_dimension(
            ['revenue', 'occupancy_pct', 'adr'],
            'city'
        )
        # city     | revenue | occupancy_pct | adr
        # Delhi    | 295M    | 60.5          | 11,450
        # Mumbai   | 669M    | 58.2          | 14,320
    """
    filters = filters or {}
    result = None

    for m in metrics:
        df, _ = execute_metric_query(m, dict(filters), group_by=group_by)
        if df is None or df.empty:
            continue
        if result is None:
            result = df.copy()
        else:
            new_cols = [c for c in df.columns if c not in result.columns]
            if new_cols:
                result = result.merge(
                    df[[group_by] + new_cols],
                    on=group_by,
                    how="outer",
                )

    return result if result is not None else pd.DataFrame()


def get_trend_data(metric_name, filters=None):
    """Weekly trend for a metric. Shortcut for group_by='week_no'."""
    return get_metric_by_dimension(metric_name, "week_no", filters)


# ════════════════════════════════════════════════
# PROPERTY PERFORMANCE TABLE
# ════════════════════════════════════════════════

def get_property_table(filters=None):
    """
    Full property performance table — all KPIs per hotel.

    Returns DataFrame with columns:
        property_name, city, category, revenue, revpar, occupancy_pct,
        adr, dsrn, dbrn, durn, realisation_pct, cancellation_pct, average_rating
    """
    filters = filters or {}

    property_metrics = [
        "revenue", "total_bookings", "adr", "average_rating",
        "realisation_pct", "cancellation_pct", "occupancy_pct",
        "revpar", "dsrn", "dbrn", "durn",
    ]

    result = get_multi_metric_by_dimension(property_metrics, "property_name", filters)

    if result.empty:
        return result

    # Enrich with city & category from dim_hotels
    hotel_info, _ = execute_custom_sql(
        "SELECT property_name, city, category FROM dim_hotels ORDER BY property_name"
    )
    if hotel_info is not None and not hotel_info.empty:
        result = result.merge(hotel_info, on="property_name", how="left")

    # Clean column order
    col_order = [
        "property_name", "city", "category", "revenue", "revpar",
        "occupancy_pct", "adr", "dsrn", "dbrn", "durn",
        "realisation_pct", "cancellation_pct", "average_rating",
    ]
    ordered = [c for c in col_order if c in result.columns]
    extra = [c for c in result.columns if c not in col_order]
    return result[ordered + extra]


# ════════════════════════════════════════════════
# BREAKDOWNS (platform / room class share)
# ════════════════════════════════════════════════

def get_platform_breakdown(filters=None):
    """Booking % by platform. Returns DataFrame with booking_platform, bookings, booking_pct."""
    df, _ = execute_metric_query("booking_pct_by_platform", dict(filters or {}))
    return df if df is not None else pd.DataFrame()


def get_room_class_breakdown(filters=None):
    """Booking % by room class. Returns DataFrame with room_class, bookings, booking_pct."""
    df, _ = execute_metric_query("booking_pct_by_room_class", dict(filters or {}))
    return df if df is not None else pd.DataFrame()


# ════════════════════════════════════════════════
# COMPARISON HELPERS
# ════════════════════════════════════════════════

def get_category_comparison(metrics=None, filters=None):
    """
    Luxury vs Business comparison across multiple metrics.

    Default metrics: revenue, occupancy_pct, adr, revpar, realisation_pct
    """
    if metrics is None:
        metrics = ["revenue", "occupancy_pct", "adr", "revpar", "realisation_pct"]
    return get_multi_metric_by_dimension(metrics, "category", filters)


def get_city_comparison(metrics=None, filters=None):
    """
    City-level comparison across multiple metrics.

    Default metrics: revenue, occupancy_pct, adr, revpar
    """
    if metrics is None:
        metrics = ["revenue", "occupancy_pct", "adr", "revpar"]
    return get_multi_metric_by_dimension(metrics, "city", filters)


def get_day_type_comparison(metrics=None, filters=None):
    """
    Weekend vs Weekday comparison.

    Default metrics: revenue, total_bookings, adr, occupancy_pct
    """
    if metrics is None:
        metrics = ["revenue", "total_bookings", "adr", "occupancy_pct"]
    return get_multi_metric_by_dimension(metrics, "day_type", filters)


# ════════════════════════════════════════════════
# PLATFORM + ADR COMBO (for Realisation% + ADR chart)
# ════════════════════════════════════════════════

def get_platform_performance(filters=None):
    """
    Realisation % and ADR by booking platform (for the combo chart).

    Returns DataFrame: booking_platform | realisation_pct | adr
    """
    return get_multi_metric_by_dimension(
        ["realisation_pct", "adr"], "booking_platform", filters
    )