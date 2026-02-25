import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from utils.metrics_engine import get_atliq_metrics, calculate_wow_delta

# 1. SETUP
load_dotenv()
st.set_page_config(page_title="AtliQ Hospitality", layout="wide")

db_uri = os.getenv("CLEAN_SUPABASE_DB_URI")
if not db_uri:
    st.error("Database URI missing in .env")
    st.stop()
engine = create_engine(db_uri)

# 2. DATA LOADING (Include category and mmm_yy)
@st.cache_data
def load_data():
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
    return pd.read_sql(q_bookings, engine), pd.read_sql(q_agg, engine)

df_b_raw, df_a_raw = load_data()

# 3. SIDEBAR FILTERS
with st.sidebar:
    st.title("Filters")
    selected_city = st.selectbox("Filter By City", ["All"] + sorted(df_b_raw['city'].dropna().unique()))
    selected_class = st.selectbox("Filter By Hotel class", ["All"] + sorted(df_b_raw['room_class'].dropna().unique()))

# 4. TOP BAR FILTERS (Month & Week Selector)
col_m, col_w = st.columns([1, 2])
with col_m:
    selected_months = st.multiselect("Select Month(s)", df_b_raw['mmm_yy'].unique(), default=[])
with col_w:
    # Sort weeks numerically 
    week_list = sorted(df_b_raw['wn'].dropna().unique())
    selected_weeks = st.multiselect("Select Week(s)", week_list, default=[])

# Apply All Filters
df_b = df_b_raw.copy()
df_a = df_a_raw.copy()

if selected_city != "All":
    df_b = df_b[df_b['city'] == selected_city]
    df_a = df_a[df_a['city'] == selected_city]
if selected_class != "All":
    df_b = df_b[df_b['room_class'] == selected_class]
    df_a = df_a[df_a['room_class'] == selected_class]
if selected_months:
    df_b = df_b[df_b['mmm_yy'].isin(selected_months)]
    df_a = df_a[df_a['mmm_yy'].isin(selected_months)]
if selected_weeks:
    df_b = df_b[df_b['wn'].isin(selected_weeks)]
    df_a = df_a[df_a['wn'].isin(selected_weeks)]

# 5. METRICS CALCULATION
m = get_atliq_metrics(df_b, df_a, df_b)
latest_wn = df_b['wn'].max()

# 6. DASHBOARD LAYOUT
st.title("AtliQ Hospitality Dashboard")

# KPI Cards
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Revenue", f"{m['revenue']/1e9:.2f}bn", calculate_wow_delta(df_b_raw, latest_wn, 'revenue'))
c2.metric("RevPar", f"{m['revpar']:,.0f}", calculate_wow_delta(df_b_raw, latest_wn, 'revenue'))
c3.metric("Occupancy %", f"{m['occ_pct']:.1f}%", calculate_wow_delta(df_b_raw, latest_wn, 'occ'))
c4.metric("ADR", f"{m['adr']:,.0f}", calculate_wow_delta(df_b_raw, latest_wn, 'adr'))
c5.metric("Realisation %", f"{m['realisation_pct']:.0f}%", "0.0%")
c6.metric("DSRN", f"{m['dsrn']:,.0f}", "0%")

st.divider()

# Charts Row 1
col_pie, col_trend = st.columns([1, 2])
with col_pie:
    st.subheader("% Revenue by Hotel Category")
    # Using 'category' (Luxury/Business) instead of room_class
    fig_pie = px.pie(df_b, values='revenue_realized', names='category', hole=0.6,
                     color_discrete_sequence=['#d65f5f', '#75797e'])
    st.plotly_chart(fig_pie, use_container_width=True)

with col_trend:
    st.subheader("Trends by Week")
    trend = df_b.groupby('wn').agg({'revenue_realized': 'sum'}).reset_index()
    st.line_chart(trend.set_index('wn'))

# Charts Row 2
col_day, col_plat = st.columns([1, 2])
with col_day:
    st.subheader("Metrics by Day Type")
    day_df = df_b.groupby('day_type').agg(
        Revenue=('revenue_realized', 'sum'),
        Bookings=('booking_id', 'count')
    ).reset_index()
    day_df['ADR'] = (day_df['Revenue'] / day_df['Bookings']).round(0)
    st.dataframe(day_df[['day_type', 'ADR', 'Revenue']], hide_index=True, use_container_width=True)

with col_plat:
    st.subheader("Realisation % and ADR by booking_platform")
    # Combo Chart Data
    plat_stats = df_b.groupby('booking_platform').apply(lambda x: pd.Series({
        'Realisation %': (1 - ((len(x[x['booking_status'] == 'Cancelled']) + len(x[x['booking_status'] == 'No Show'])) / max(len(x), 1))) * 100,
        'ADR': x['revenue_realized'].sum() / max(len(x), 1)
    })).reset_index()
    
    # Dual Axis Plotly Chart
    fig_plat = go.Figure()
    fig_plat.add_trace(go.Bar(
        x=plat_stats['booking_platform'], y=plat_stats['Realisation %'],
        name='Realisation %', marker_color='#d65f5f',
        text=plat_stats['Realisation %'].round(2).astype(str) + '%', textposition='inside'
    ))
    fig_plat.add_trace(go.Scatter(
        x=plat_stats['booking_platform'], y=plat_stats['ADR'],
        name='ADR', yaxis='y2', line=dict(color='#4c4c4c', width=3), mode='lines+markers'
    ))
    fig_plat.update_layout(
        yaxis=dict(title="Realisation %", range=[0, 100]),
        yaxis2=dict(title="ADR", overlaying='y', side='right', showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_plat, use_container_width=True)

# Full Property Performance Table
st.subheader("Property Performance Table")

# Vectorized grouping for speed and safety
b_grp = df_b.groupby(['property_id', 'property_name', 'city']).agg(
    Revenue=('revenue_realized', 'sum'),
    Bookings=('booking_id', 'count'),
    Cancelled=('booking_status', lambda x: (x == 'Cancelled').sum()),
    No_Show=('booking_status', lambda x: (x == 'No Show').sum()),
    Checked_Out=('booking_status', lambda x: (x == 'Checked Out').sum()),
    Ratings=('ratings_given', 'mean')
).reset_index()

a_grp = df_a.groupby('property_id').agg(
    Capacity=('capacity', 'sum'),
    Succ_Bookings=('successful_bookings', 'sum')
).reset_index()

# Merge and calculate final columns
prop_merged = pd.merge(b_grp, a_grp, on='property_id', how='left').fillna(0)

prop_merged['Revenue'] = prop_merged['Revenue'].apply(lambda x: f"{x/1e6:.0f}M")
prop_merged['RevPAR'] = (prop_merged['Revenue'].str.replace('M','').astype(float) * 1e6 / prop_merged['Capacity'].replace(0, 1)).round(0)
prop_merged['Occupancy %'] = ((prop_merged['Succ_Bookings'] / prop_merged['Capacity'].replace(0, 1)) * 100).round(0).astype(str) + "%"
prop_merged['ADR'] = (prop_merged['Revenue'].str.replace('M','').astype(float) * 1e6 / prop_merged['Bookings'].replace(0, 1)).round(2)
prop_merged['DSRN'] = (prop_merged['Capacity'] / m['no_of_days']).round(0)
prop_merged['DBRN'] = (prop_merged['Bookings'] / m['no_of_days']).round(2)
prop_merged['DURN'] = (prop_merged['Checked_Out'] / m['no_of_days']).round(0)
prop_merged['Realisation %'] = ((1 - ((prop_merged['Cancelled'] + prop_merged['No_Show']) / prop_merged['Bookings'].replace(0, 1))) * 100).round(0).astype(str) + "%"
prop_merged['Cancellation %'] = ((prop_merged['Cancelled'] / prop_merged['Bookings'].replace(0, 1)) * 100).round(0).astype(str) + "%"
prop_merged['Average Rating'] = prop_merged['Ratings'].round(2)

display_cols = ['property_id', 'property_name', 'city', 'Revenue', 'RevPAR', 'Occupancy %', 'ADR', 'DSRN', 'DBRN', 'DURN', 'Realisation %', 'Cancellation %', 'Average Rating']
st.dataframe(prop_merged[display_cols], hide_index=True, use_container_width=True)