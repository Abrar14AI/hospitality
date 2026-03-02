# frontend/dashboard.py
"""
AtliQ Hospitality Executive Dashboard

Powered by the shared metrics engine (same SQL as the chat agent).
All KPIs calculated via deterministic SQL builder — numbers always match.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils.metrics_engine import (
    get_core_metrics,
    get_all_wow_deltas,
    get_latest_full_week,
    get_db_context,
    get_trend_data,
    get_category_comparison,
    get_city_comparison,
    get_day_type_comparison,
    get_platform_performance,
    get_property_table,
    get_metric_by_dimension,
    build_dashboard_filters,
)


# ════════════════════════════════════════════════
# PAGE CONFIG
# ════════════════════════════════════════════════

st.set_page_config(
    page_title="AtliQ Hospitality Dashboard",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better KPI cards
st.markdown("""
<style>
    [data-testid="stMetricValue"] { font-size: 1.8rem; }
    [data-testid="stMetricDelta"] { font-size: 1rem; }
    .block-container { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════
# LOAD DATABASE CONTEXT
# ════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_db_context():
    """Load available filter options from database."""
    return get_db_context()


ctx = load_db_context()

if "error" in ctx:
    st.error(f"Database connection failed: {ctx['error']}")
    st.stop()

# Extract filter options
available_weeks = sorted(ctx.get("weeks", []), key=lambda x: int(x))
available_cities = ctx.get("cities", [])
latest_full_week = ctx.get("latest_full_week", available_weeks[-1] if available_weeks else None)


# ════════════════════════════════════════════════
# SIDEBAR FILTERS
# ════════════════════════════════════════════════

with st.sidebar:
    st.title("🎛️ Filters")
    st.markdown("---")

    # City filter
    selected_city = st.selectbox(
        "City",
        options=["All"] + available_cities,
        index=0,
    )

    # Category filter (Luxury / Business)
    selected_category = st.selectbox(
        "Hotel Category",
        options=["All", "Luxury", "Business"],
        index=0,
    )

    # Room class filter
    selected_room_class = st.selectbox(
        "Room Class",
        options=["All", "Standard", "Elite", "Premium", "Presidential"],
        index=0,
    )

    st.markdown("---")

    # Month filter
    selected_month = st.selectbox(
        "Month",
        options=["All", "May 22", "Jun 22", "Jul 22"],
        index=0,
    )

    # Week filter
    selected_week = st.selectbox(
        "Week",
        options=["All"] + available_weeks,
        index=0,
        help=f"Latest full week: {latest_full_week}",
    )

    st.markdown("---")
    st.caption(f"Data: {ctx.get('date_range', 'N/A')}")
    st.caption(f"Latest full week: {latest_full_week}")


# ════════════════════════════════════════════════
# BUILD FILTERS DICT
# ════════════════════════════════════════════════

filters = build_dashboard_filters(
    city=selected_city,
    category=selected_category,
    room_class=selected_room_class,
    month=selected_month if selected_month != "All" else None,
    week=selected_week if selected_week != "All" else None,
)

# Determine which week to use for WoW calculations
wow_week = selected_week if selected_week != "All" else latest_full_week


# ════════════════════════════════════════════════
# LOAD METRICS
# ════════════════════════════════════════════════

@st.cache_data(ttl=60)
def load_metrics(_filters_tuple):
    """Load all core metrics with caching. _filters_tuple for hashability."""
    filters_dict = dict(_filters_tuple)
    return get_core_metrics(filters_dict)


@st.cache_data(ttl=60)
def load_wow_deltas(_wow_week, _filters_tuple):
    """Load WoW deltas with caching."""
    filters_dict = dict(_filters_tuple)
    return get_all_wow_deltas(_wow_week, filters_dict)


# Convert filters dict to tuple for caching
filters_tuple = tuple(sorted(filters.items()))

metrics = load_metrics(filters_tuple)
wow = load_wow_deltas(wow_week, filters_tuple)


# ════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════

st.title("🏨 AtliQ Hospitality Dashboard")

# Active filters display
active_filters = [f"{k}: {v}" for k, v in filters.items()]
if active_filters:
    st.caption(f"Filters: {' | '.join(active_filters)}")
else:
    st.caption("Showing: All Data")

st.markdown("---")


# ════════════════════════════════════════════════
# KPI CARDS (Row 1)
# ════════════════════════════════════════════════

def format_revenue(val):
    """Format revenue in readable form."""
    if val >= 1e9:
        return f"₹{val/1e9:.2f}B"
    elif val >= 1e6:
        return f"₹{val/1e6:.1f}M"
    elif val >= 1e3:
        return f"₹{val/1e3:.1f}K"
    return f"₹{val:.0f}"


def format_rate(val):
    """Format ADR/RevPAR."""
    return f"₹{val:,.0f}"


def format_pct(val):
    """Format percentage."""
    return f"{val:.1f}%"


col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.metric(
        label="Revenue",
        value=format_revenue(metrics.get("revenue", 0)),
        delta=wow.get("revenue", "N/A"),
    )

with col2:
    st.metric(
        label="RevPAR",
        value=format_rate(metrics.get("revpar", 0)),
        delta=wow.get("revpar", "N/A"),
    )

with col3:
    st.metric(
        label="Occupancy %",
        value=format_pct(metrics.get("occupancy_pct", 0)),
        delta=wow.get("occupancy", "N/A"),
    )

with col4:
    st.metric(
        label="ADR",
        value=format_rate(metrics.get("adr", 0)),
        delta=wow.get("adr", "N/A"),
    )

with col5:
    st.metric(
        label="Realisation %",
        value=format_pct(metrics.get("realisation_pct", 0)),
        delta=wow.get("realisation", "N/A"),
    )

with col6:
    st.metric(
        label="DSRN",
        value=f"{metrics.get('dsrn', 0):,.0f}",
        delta=wow.get("dsrn", "N/A"),
    )


st.markdown("---")


# ════════════════════════════════════════════════
# ROW 2: Category Breakdown + Trend
# ════════════════════════════════════════════════

col_pie, col_trend = st.columns([1, 2])

with col_pie:
    st.subheader("Revenue by Category")

    @st.cache_data(ttl=60)
    def load_category_revenue(_filters_tuple):
        f = dict(_filters_tuple)
        return get_metric_by_dimension("revenue", "category", f)

    cat_df = load_category_revenue(filters_tuple)

    if not cat_df.empty:
        fig_pie = px.pie(
            cat_df,
            values="revenue",
            names="category",
            hole=0.55,
            color="category",
            color_discrete_map={"Luxury": "#5D5FEF", "Business": "#7EC8E3"},
        )
        fig_pie.update_traces(
            textposition="inside",
            textinfo="percent+label",
            textfont_size=12,
        )
        fig_pie.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20, l=20, r=20),
            height=300,
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No data available for selected filters.")


with col_trend:
    st.subheader("Weekly Revenue Trend")

    @st.cache_data(ttl=60)
    def load_revenue_trend(_filters_tuple):
        f = dict(_filters_tuple)
        # Remove week filter for trend view
        f_trend = {k: v for k, v in f.items() if k != "week_no"}
        return get_trend_data("revenue", f_trend)

    trend_df = load_revenue_trend(filters_tuple)

    if not trend_df.empty:
        # Sort by week number
        trend_df["week_sort"] = trend_df["week_no"].astype(int)
        trend_df = trend_df.sort_values("week_sort")
        trend_df["week_label"] = "W" + trend_df["week_no"].astype(str)

        fig_trend = px.line(
            trend_df,
            x="week_label",
            y="revenue",
            markers=True,
            color_discrete_sequence=["#5D5FEF"],
        )
        fig_trend.update_traces(
            line_width=3,
            marker_size=8,
            hovertemplate="Week %{x}<br>Revenue: ₹%{y:,.0f}<extra></extra>",
        )
        fig_trend.update_layout(
            xaxis_title="Week",
            yaxis_title="Revenue (₹)",
            margin=dict(t=20, b=40, l=60, r=20),
            height=300,
            hovermode="x unified",
        )
        fig_trend.update_yaxes(tickformat=",")
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No trend data available.")


st.markdown("---")


# ════════════════════════════════════════════════
# ROW 3: Day Type Analysis + Platform Performance
# ════════════════════════════════════════════════

col_day, col_plat = st.columns([1, 2])

with col_day:
    st.subheader("Weekend vs Weekday")

    @st.cache_data(ttl=60)
    def load_day_comparison(_filters_tuple):
        f = dict(_filters_tuple)
        return get_day_type_comparison(
            ["revenue", "total_bookings", "adr", "occupancy_pct"], f
        )

    day_df = load_day_comparison(filters_tuple)

    if not day_df.empty:
        # Format for display
        display_df = day_df.copy()
        display_df["revenue"] = display_df["revenue"].apply(
            lambda x: f"₹{x/1e6:.1f}M" if x else "₹0"
        )
        display_df["adr"] = display_df["adr"].apply(
            lambda x: f"₹{x:,.0f}" if x else "₹0"
        )
        display_df["occupancy_pct"] = display_df["occupancy_pct"].apply(
            lambda x: f"{x:.1f}%" if x else "0%"
        )
        display_df["total_bookings"] = display_df["total_bookings"].apply(
            lambda x: f"{int(x):,}" if x else "0"
        )
        display_df = display_df.rename(columns={
            "day_type": "Day Type",
            "revenue": "Revenue",
            "total_bookings": "Bookings",
            "adr": "ADR",
            "occupancy_pct": "Occupancy",
        })
        st.dataframe(
            display_df,
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No data available.")


with col_plat:
    st.subheader("Realisation % & ADR by Platform")

    @st.cache_data(ttl=60)
    def load_platform_perf(_filters_tuple):
        f = dict(_filters_tuple)
        return get_platform_performance(f)

    plat_df = load_platform_perf(filters_tuple)

    if not plat_df.empty:
        plat_df = plat_df.sort_values("realisation_pct", ascending=False)

        fig_combo = go.Figure()

        # Bar: Realisation %
        fig_combo.add_trace(go.Bar(
            x=plat_df["booking_platform"],
            y=plat_df["realisation_pct"],
            name="Realisation %",
            marker_color="#5D5FEF",
            text=plat_df["realisation_pct"].apply(lambda x: f"{x:.1f}%"),
            textposition="inside",
            textfont_color="white",
            yaxis="y",
        ))

        # Line: ADR
        fig_combo.add_trace(go.Scatter(
            x=plat_df["booking_platform"],
            y=plat_df["adr"],
            name="ADR",
            mode="lines+markers",
            line=dict(color="#FF6B6B", width=3),
            marker=dict(size=8),
            yaxis="y2",
        ))

        fig_combo.update_layout(
            yaxis=dict(
                title="Realisation %",
                range=[0, 100],
                title_font=dict(color="#5D5FEF"),
                tickfont=dict(color="#5D5FEF"),
            ),
            yaxis2=dict(
                title="ADR (₹)",
                overlaying="y",
                side="right",
                showgrid=False,
                title_font=dict(color="#FF6B6B"),
                tickfont=dict(color="#FF6B6B"),
                tickformat=",",
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            margin=dict(t=40, b=40, l=60, r=60),
            height=300,
            hovermode="x unified",
        )

        st.plotly_chart(fig_combo, use_container_width=True)
    else:
        st.info("No platform data available.")


st.markdown("---")


# ════════════════════════════════════════════════
# ROW 4: City Comparison + Occupancy Trend
# ════════════════════════════════════════════════

col_city, col_occ = st.columns(2)

with col_city:
    st.subheader("City Performance")

    @st.cache_data(ttl=60)
    def load_city_comparison(_filters_tuple):
        f = dict(_filters_tuple)
        # Remove city filter for comparison
        f_city = {k: v for k, v in f.items() if k != "city"}
        return get_city_comparison(
            ["revenue", "occupancy_pct", "adr", "revpar"], f_city
        )

    city_df = load_city_comparison(filters_tuple)

    if not city_df.empty:
        city_df = city_df.sort_values("revenue", ascending=True)

        fig_city = go.Figure()

        fig_city.add_trace(go.Bar(
            y=city_df["city"],
            x=city_df["revenue"],
            orientation="h",
            name="Revenue",
            marker_color="#5D5FEF",
            text=city_df["revenue"].apply(lambda x: f"₹{x/1e6:.0f}M"),
            textposition="outside",
        ))

        fig_city.update_layout(
            xaxis_title="Revenue (₹)",
            yaxis_title="",
            margin=dict(t=20, b=40, l=100, r=80),
            height=280,
            showlegend=False,
        )
        fig_city.update_xaxes(tickformat=",")

        st.plotly_chart(fig_city, use_container_width=True)
    else:
        st.info("No city data available.")


with col_occ:
    st.subheader("Weekly Occupancy Trend")

    @st.cache_data(ttl=60)
    def load_occupancy_trend(_filters_tuple):
        f = dict(_filters_tuple)
        f_trend = {k: v for k, v in f.items() if k != "week_no"}
        return get_trend_data("occupancy_pct", f_trend)

    occ_df = load_occupancy_trend(filters_tuple)

    if not occ_df.empty:
        occ_df["week_sort"] = occ_df["week_no"].astype(int)
        occ_df = occ_df.sort_values("week_sort")
        occ_df["week_label"] = "W" + occ_df["week_no"].astype(str)

        fig_occ = px.area(
            occ_df,
            x="week_label",
            y="occupancy_pct",
            color_discrete_sequence=["#7EC8E3"],
        )
        fig_occ.update_traces(
            line_width=2,
            hovertemplate="Week %{x}<br>Occupancy: %{y:.1f}%<extra></extra>",
        )
        fig_occ.update_layout(
            xaxis_title="Week",
            yaxis_title="Occupancy %",
            yaxis_range=[0, 100],
            margin=dict(t=20, b=40, l=60, r=20),
            height=280,
        )

        st.plotly_chart(fig_occ, use_container_width=True)
    else:
        st.info("No occupancy data available.")


st.markdown("---")


# ════════════════════════════════════════════════
# ROW 5: Property Performance Table
# ════════════════════════════════════════════════

st.subheader("🏢 Property Performance")

@st.cache_data(ttl=60)
def load_property_table(_filters_tuple):
    f = dict(_filters_tuple)
    return get_property_table(f)


prop_df = load_property_table(filters_tuple)

if not prop_df.empty:
    # Format columns for display
    display_prop = prop_df.copy()

    # Format revenue
    if "revenue" in display_prop.columns:
        display_prop["revenue"] = display_prop["revenue"].apply(
            lambda x: f"₹{x/1e6:.1f}M" if pd.notna(x) and x else "₹0"
        )

    # Format RevPAR & ADR
    for col in ["revpar", "adr"]:
        if col in display_prop.columns:
            display_prop[col] = display_prop[col].apply(
                lambda x: f"₹{x:,.0f}" if pd.notna(x) and x else "₹0"
            )

    # Format percentages
    for col in ["occupancy_pct", "realisation_pct", "cancellation_pct"]:
        if col in display_prop.columns:
            display_prop[col] = display_prop[col].apply(
                lambda x: f"{x:.1f}%" if pd.notna(x) else "0%"
            )

    # Format daily metrics
    for col in ["dsrn", "dbrn", "durn"]:
        if col in display_prop.columns:
            display_prop[col] = display_prop[col].apply(
                lambda x: f"{x:,.1f}" if pd.notna(x) else "0"
            )

    # Format rating
    if "average_rating" in display_prop.columns:
        display_prop["average_rating"] = display_prop["average_rating"].apply(
            lambda x: f"{x:.2f}" if pd.notna(x) and x else "N/A"
        )

    # Rename columns for display
    column_names = {
        "property_name": "Property",
        "city": "City",
        "category": "Category",
        "revenue": "Revenue",
        "revpar": "RevPAR",
        "occupancy_pct": "Occupancy",
        "adr": "ADR",
        "dsrn": "DSRN",
        "dbrn": "DBRN",
        "durn": "DURN",
        "realisation_pct": "Realisation",
        "cancellation_pct": "Cancellation",
        "average_rating": "Rating",
    }
    display_prop = display_prop.rename(columns=column_names)

    st.dataframe(
        display_prop,
        hide_index=True,
        use_container_width=True,
        height=400,
    )

    # Summary stats
    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
    with col_stat1:
        st.caption(f"Total Properties: {len(prop_df)}")
    with col_stat2:
        if "category" in prop_df.columns:
            lux = len(prop_df[prop_df["category"] == "Luxury"])
            st.caption(f"Luxury: {lux}")
    with col_stat3:
        if "category" in prop_df.columns:
            biz = len(prop_df[prop_df["category"] == "Business"])
            st.caption(f"Business: {biz}")
    with col_stat4:
        st.caption(f"Cities: {prop_df['city'].nunique() if 'city' in prop_df.columns else 'N/A'}")

else:
    st.info("No property data available for selected filters.")


# ════════════════════════════════════════════════
# FOOTER
# ════════════════════════════════════════════════

st.markdown("---")
st.caption(
    "📊 Data powered by AtliQ Hospitality Metrics Engine | "
    f"Showing data for weeks {available_weeks[0]}–{available_weeks[-1]} (2022)"
)