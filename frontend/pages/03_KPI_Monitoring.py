# pages/03_KPI_Monitoring.py
import streamlit as st
import pandas as pd
from datetime import datetime
from utils.metrics_engine import get_atliq_metrics, calculate_wow_delta
from agents.agents_system import query_data_agent  # optional for advanced alerts

st.title("🔍 KPI Monitoring & Anomaly Detection")

st.markdown("This page runs autonomous monitoring of key hotel KPIs. It checks thresholds, detects trends/anomalies, and suggests actions.")

# Load data (reuse your existing logic)
@st.cache_data(ttl=300)
def load_monitor_data():
    q_bookings = """
    SELECT fb.*, dh.city, dh.property_name, dh.category, dr.room_class, dd.date, dd.week_no as wn, dd.mmm_yy, dd.day_type 
    FROM fact_bookings fb
    JOIN dim_hotels dh ON fb.property_id = dh.property_id
    JOIN dim_rooms dr ON fb.room_category = dr.room_id
    JOIN dim_date dd ON fb.check_in_date = dd.date
    """
    q_agg = """
    SELECT fa.*, dh.city, dr.room_class, dd.week_no as wn, dd.mmm_yy 
    FROM fact_aggregated_bookings fa
    JOIN dim_hotels dh ON fa.property_id = dh.property_id
    JOIN dim_rooms dr ON fa.room_category = dr.room_id
    JOIN dim_date dd ON fa.check_in_date = dd.date
    """
    df_b = pd.read_sql(q_bookings, st.session_state.engine if 'engine' in st.session_state else None)
    df_a = pd.read_sql(q_agg, st.session_state.engine if 'engine' in st.session_state else None)
    return df_b, df_a

df_b, df_a = load_monitor_data()

if df_b.empty or df_a.empty:
    st.error("No data available. Please check ETL and database connection.")
    st.stop()

# Calculate current metrics
metrics = get_atliq_metrics(df_b, df_a, df_b)
latest_wn = df_b['wn'].max() if 'wn' in df_b.columns else None

# Threshold-based alerts
alerts = []

if metrics.get('occ_pct', 0) < 60:
    alerts.append(("warning", f"Low Occupancy: {metrics['occ_pct']:.1f}% (below 60% threshold)"))

if metrics.get('cancellation_pct', 0) > 25:
    alerts.append(("error", f"High Cancellation Rate: {metrics['cancellation_pct']:.1f}% (above 25%)"))

if metrics.get('realisation_pct', 0) < 70:
    alerts.append(("warning", f"Low Realisation: {metrics['realisation_pct']:.1f}% (below 70%)"))

if metrics.get('avg_rating', 0) < 3.5:
    alerts.append(("error", f"Low Average Rating: {metrics['avg_rating']:.2f} (below 3.5)"))

# WoW trend alerts
if latest_wn:
    wow_rev = calculate_wow_delta(df_b, latest_wn, 'revenue')
    wow_occ = calculate_wow_delta(df_b, latest_wn, 'occ')
    
    if float(wow_rev.strip('%')) < -10:
        alerts.append(("error", f"Revenue down {wow_rev} WoW"))
    if float(wow_occ.strip('%')) < -5:
        alerts.append(("warning", f"Occupancy down {wow_occ} WoW"))

# Display alerts
if alerts:
    st.subheader("Active Alerts")
    for level, msg in alerts:
        if level == "error":
            st.error(msg)
        elif level == "warning":
            st.warning(msg)
else:
    st.success("All KPIs within normal range.")

# Monitoring controls
if st.button("Run Full KPI Analysis Now"):
    with st.spinner("Analyzing KPIs..."):
        # You can call your agent here for deeper analysis if desired
        # response = query_data_agent("Perform full KPI health check and list any concerning trends")
        # st.markdown(response)
        st.success("Analysis complete. See alerts above.")

# Simple trend visualization
st.subheader("Recent Occupancy Trend")
if 'wn' in df_b.columns:
    trend = df_b.groupby('wn')['occupancy_pct'].mean().reset_index()
    fig = px.line(trend, x='wn', y='occupancy_pct', markers=True)
    st.plotly_chart(fig, use_container_width=True)

st.caption(f"Last check: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")