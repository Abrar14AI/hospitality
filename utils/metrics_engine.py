import pandas as pd
import numpy as np

def get_atliq_metrics(df_bookings, df_agg, df_date):
    """
    Implements the 20 base measures and 2 calculated columns 
    exactly as defined in the DAX documentation.
    """
    m = {}
    
    # 1. Calculated Columns (Foundation)
    # wn = WEEKNUM
    if 'date' in df_date.columns:
        df_date['date'] = pd.to_datetime(df_date['date'])
        df_date['wn'] = df_date['date'].dt.isocalendar().week
        
        # day_type: Friday(4) and Saturday(5) in Python dt.weekday are 4,5. 
        # But your DAX uses WEEKDAY(date, 2) where Mon=1...Sun=7. 
        # In that case, >5 means Sat(6) and Sun(7). 
        # ADJUSTMENT: We follow your DAX var wkd = WEEKDAY(date,2) logic.
        df_date['wkd'] = df_date['date'].dt.weekday + 1 # Mon=1, Sun=7
        df_date['day_type'] = df_date['wkd'].apply(lambda x: "Weekend" if x > 5 else "Weekday")

    # 2. Base Measures
    m['revenue'] = df_bookings['revenue_realized'].sum()
    m['total_bookings'] = len(df_bookings)
    m['total_capacity'] = df_agg['capacity'].sum()
    m['total_succ_bookings'] = df_agg['successful_bookings'].sum()
    
    # Occupancy % = DIVIDE([Total Successful Bookings],[Total Capacity],0)
    m['occ_pct'] = (m['total_succ_bookings'] / m['total_capacity']) * 100 if m['total_capacity'] > 0 else 0
    m['avg_rating'] = df_bookings['ratings_given'].mean()
    
    # No of days = DATEDIFF
    m['no_of_days'] = (df_date['date'].max() - df_date['date'].min()).days + 1
    
    # Status Counts
    m['cancelled'] = len(df_bookings[df_bookings['booking_status'] == "Cancelled"])
    m['checked_out'] = len(df_bookings[df_bookings['booking_status'] == "Checked Out"])
    m['no_show'] = len(df_bookings[df_bookings['booking_status'] == "No Show"])
    
    # Rates
    m['cancellation_pct'] = (m['cancelled'] / m['total_bookings']) * 100 if m['total_bookings'] > 0 else 0
    m['no_show_rate_pct'] = (m['no_show'] / m['total_bookings']) * 100 if m['total_bookings'] > 0 else 0
    m['realisation_pct'] = 100 - (m['cancellation_pct'] + m['no_show_rate_pct'])
    
    # Hospitality Ratios
    m['adr'] = m['revenue'] / m['total_bookings'] if m['total_bookings'] > 0 else 0
    m['revpar'] = m['revenue'] / m['total_capacity'] if m['total_capacity'] > 0 else 0
    
    # Daily Metrics
    m['dbrn'] = m['total_bookings'] / m['no_of_days']
    m['dsrn'] = m['total_capacity'] / m['no_of_days']
    m['durn'] = m['checked_out'] / m['no_of_days']
    
    return m

def calculate_wow_metrics(df_bookings, df_agg, df_date, target_week=None):
    """
    Implements the 6 WoW Change % measures.
    """
    if target_week is None:
        target_week = df_date['wn'].max()
    
    # Get CW (Current Week) and PW (Previous Week) data
    def get_week_metrics(week):
        # Filter dataframes for the specific week
        b_w = df_bookings[df_bookings['wn'] == week]
        a_w = df_agg[df_agg['wn'] == week]
        d_w = df_date[df_date['wn'] == week]
        return get_atliq_metrics(b_w, a_w, d_w)

    cw = get_week_metrics(target_week)
    pw = get_week_metrics(target_week - 1)
    
    wow = {}
    metrics_to_track = ['revenue', 'occ_pct', 'adr', 'revpar', 'realisation_pct', 'dsrn']
    
    for metric in metrics_to_track:
        current_val = cw.get(metric, 0)
        previous_val = pw.get(metric, 0)
        
        # DIVIDE(cw, pw, 0) - 1
        if previous_val > 0:
            wow[f'{metric}_wow'] = (current_val / previous_val) - 1
        else:
            wow[f'{metric}_wow'] = 0
            
    return wow