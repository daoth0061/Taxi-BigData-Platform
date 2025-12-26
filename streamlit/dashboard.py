"""
NYC Taxi Analytics Dashboard
============================
A business-story dashboard with 4 interconnected sections:
1. Executive Overview - KPIs at a glance
2. Revenue Deep-Dive - Where does money come from?
3. Operations & Efficiency - How efficient are our trips?
4. Geographic Intelligence - Where are the hotspots?
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from pyhive import hive
from contextlib import contextmanager

# =============================================================================
# CONFIGURATION
# =============================================================================
SPARK_HOST = "spark-thrift"
SPARK_PORT = 10000
DATABASE = "lakehouse.gold"

# Color theme - Professional dark theme
COLORS = {
    "primary": "#4F46E5",      # Indigo
    "secondary": "#7C3AED",    # Violet
    "success": "#10B981",      # Emerald
    "warning": "#F59E0B",      # Amber
    "danger": "#EF4444",       # Red
    "info": "#06B6D4",         # Cyan
    "background": "#0F172A",   # Slate 900
    "card": "#1E293B",         # Slate 800
    "text": "#F8FAFC",         # Slate 50
    "muted": "#94A3B8",        # Slate 400
}

PAYMENT_COLORS = {
    "Credit card": "#4F46E5",
    "Cash": "#10B981",
    "No charge": "#F59E0B",
    "Dispute": "#EF4444",
    "Unknown": "#6B7280",
}

ZONE_COLORS = {
    "Manhattan": "#4F46E5",
    "Brooklyn": "#7C3AED",
    "Queens": "#10B981",
    "Bronx": "#F59E0B",
    "Staten Island": "#06B6D4",
    "Unknown": "#6B7280",
}

# =============================================================================
# DATABASE CONNECTION
# =============================================================================
@contextmanager
def get_connection():
    """Create a connection to Spark Thrift Server"""
    conn = None
    try:
        conn = hive.Connection(
            host=SPARK_HOST,
            port=SPARK_PORT,
            username="hive"
        )
        yield conn
    finally:
        if conn:
            conn.close()

@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    """Execute SQL query and return DataFrame"""
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

# =============================================================================
# DATA QUERIES
# =============================================================================
def get_executive_kpis() -> dict:
    """Fetch executive overview KPIs"""
    query = """
    SELECT 
        SUM(total_amount) as total_revenue,
        COUNT(*) as total_trips,
        AVG(total_amount) as avg_revenue_per_trip,
        AVG(fare_amount) as avg_fare,
        AVG(tip_amount) as avg_tip,
        SUM(passenger_count) as total_passengers,
        AVG(trip_distance) as avg_distance,
        AVG(trip_duration_minutes) as avg_duration
    FROM gold_fact_trip
    """
    df = run_query(query)
    if not df.empty:
        return df.iloc[0].to_dict()
    return {}

def get_rush_hour_comparison() -> pd.DataFrame:
    """Compare rush hour vs off-peak metrics"""
    query = """
    SELECT 
        d.is_rush_hour,
        COUNT(*) as trips,
        SUM(f.total_amount) as revenue,
        AVG(f.total_amount) as avg_revenue,
        AVG(f.trip_duration_minutes) as avg_duration
    FROM gold_fact_trip f
    JOIN gold_dim_datetime d ON f.pickup_datetime_sk = d.datetime_sk
    GROUP BY d.is_rush_hour
    """
    return run_query(query)

def get_revenue_by_payment() -> pd.DataFrame:
    """Revenue breakdown by payment type"""
    query = """
    SELECT 
        payment_type,
        SUM(total_amount) as revenue,
        COUNT(*) as trips,
        AVG(tip_amount) as avg_tip
    FROM gold_fact_trip
    GROUP BY payment_type
    ORDER BY revenue DESC
    """
    return run_query(query)

def get_revenue_by_day_type() -> pd.DataFrame:
    """Revenue by weekday vs weekend"""
    query = """
    SELECT 
        d.is_weekend,
        d.day_name,
        SUM(f.total_amount) as revenue,
        COUNT(*) as trips
    FROM gold_fact_trip f
    JOIN gold_dim_datetime d ON f.pickup_datetime_sk = d.datetime_sk
    GROUP BY d.is_weekend, d.day_name
    ORDER BY d.is_weekend, revenue DESC
    """
    return run_query(query)

def get_hourly_pattern() -> pd.DataFrame:
    """Hourly revenue and trip pattern"""
    query = """
    SELECT 
        d.hour,
        SUM(f.total_amount) as revenue,
        COUNT(*) as trips,
        AVG(f.total_amount) as avg_revenue
    FROM gold_fact_trip f
    JOIN gold_dim_datetime d ON f.pickup_datetime_sk = d.datetime_sk
    GROUP BY d.hour
    ORDER BY d.hour
    """
    return run_query(query)

def get_top_routes() -> pd.DataFrame:
    """Top 5 revenue routes with zone names"""
    query = """
    SELECT 
        pz.zone_name as pickup_zone,
        dz.zone_name as dropoff_zone,
        SUM(f.total_amount) as revenue,
        COUNT(*) as trips,
        AVG(f.total_amount) as avg_fare
    FROM gold_fact_trip f
    JOIN gold_dim_zone pz ON f.pickup_zone_id = pz.zone_id
    JOIN gold_dim_zone dz ON f.dropoff_zone_id = dz.zone_id
    GROUP BY pz.zone_name, dz.zone_name
    ORDER BY revenue DESC
    LIMIT 10
    """
    return run_query(query)

def get_tip_by_payment() -> pd.DataFrame:
    """Tip percentage by payment type"""
    query = """
    SELECT 
        payment_type,
        AVG(tip_amount) as avg_tip,
        AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount * 100 ELSE 0 END) as tip_percentage,
        COUNT(*) as trips
    FROM gold_fact_trip
    GROUP BY payment_type
    ORDER BY avg_tip DESC
    """
    return run_query(query)

def get_duration_distribution() -> pd.DataFrame:
    """Trip duration distribution"""
    query = """
    SELECT 
        CASE 
            WHEN trip_duration_minutes < 5 THEN '0-5 min'
            WHEN trip_duration_minutes < 10 THEN '5-10 min'
            WHEN trip_duration_minutes < 15 THEN '10-15 min'
            WHEN trip_duration_minutes < 20 THEN '15-20 min'
            WHEN trip_duration_minutes < 30 THEN '20-30 min'
            ELSE '30+ min'
        END as duration_bucket,
        COUNT(*) as trips
    FROM gold_fact_trip
    GROUP BY 
        CASE 
            WHEN trip_duration_minutes < 5 THEN '0-5 min'
            WHEN trip_duration_minutes < 10 THEN '5-10 min'
            WHEN trip_duration_minutes < 15 THEN '10-15 min'
            WHEN trip_duration_minutes < 20 THEN '15-20 min'
            WHEN trip_duration_minutes < 30 THEN '20-30 min'
            ELSE '30+ min'
        END
    ORDER BY MIN(trip_duration_minutes)
    """
    return run_query(query)

def get_zone_heatmap() -> pd.DataFrame:
    """Zone-to-zone trip flow for heatmap"""
    query = """
    SELECT 
        pz.zone_name as pickup_zone,
        dz.zone_name as dropoff_zone,
        COUNT(*) as trips,
        SUM(f.total_amount) as revenue
    FROM gold_fact_trip f
    JOIN gold_dim_zone pz ON f.pickup_zone_id = pz.zone_id
    JOIN gold_dim_zone dz ON f.dropoff_zone_id = dz.zone_id
    GROUP BY pz.zone_name, dz.zone_name
    """
    return run_query(query)

def get_zone_performance() -> pd.DataFrame:
    """Zone performance summary"""
    query = """
    SELECT 
        pz.zone_name as zone,
        COUNT(*) as total_trips,
        SUM(f.total_amount) as total_revenue,
        AVG(f.fare_amount) as avg_fare,
        AVG(f.tip_amount) as avg_tip,
        AVG(f.trip_distance) as avg_distance
    FROM gold_fact_trip f
    JOIN gold_dim_zone pz ON f.pickup_zone_id = pz.zone_id
    GROUP BY pz.zone_name
    ORDER BY total_revenue DESC
    """
    return run_query(query)

# =============================================================================
# CHART COMPONENTS
# =============================================================================
def create_kpi_card(title: str, value: str, subtitle: str = "", delta: str = None):
    """Create a styled KPI card"""
    delta_html = ""
    if delta:
        delta_color = COLORS["success"] if delta.startswith("+") else COLORS["danger"]
        delta_html = f'<span style="color: {delta_color}; font-size: 14px;">{delta}</span>'
    
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {COLORS['card']} 0%, {COLORS['background']} 100%);
        border-radius: 12px;
        padding: 20px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
    ">
        <p style="color: {COLORS['muted']}; margin: 0; font-size: 14px; text-transform: uppercase; letter-spacing: 1px;">{title}</p>
        <h2 style="color: {COLORS['text']}; margin: 8px 0; font-size: 32px; font-weight: 700;">{value}</h2>
        <p style="color: {COLORS['muted']}; margin: 0; font-size: 12px;">{subtitle} {delta_html}</p>
    </div>
    """, unsafe_allow_html=True)

def section_header(emoji: str, title: str, subtitle: str):
    """Create a section header"""
    st.markdown(f"""
    <div style="margin: 40px 0 20px 0;">
        <h2 style="color: {COLORS['text']}; margin: 0;">
            {emoji} {title}
        </h2>
        <p style="color: {COLORS['muted']}; margin: 5px 0 0 0; font-style: italic;">
            {subtitle}
        </p>
    </div>
    """, unsafe_allow_html=True)

# =============================================================================
# MAIN DASHBOARD
# =============================================================================
def main():
    # Page config
    st.set_page_config(
        page_title="NYC Taxi Analytics",
        page_icon="",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Custom CSS for dark theme
    st.markdown(f"""
    <style>
        .stApp {{
            background-color: {COLORS['background']};
        }}
        .stMetric {{
            background-color: {COLORS['card']};
            padding: 15px;
            border-radius: 10px;
        }}
        h1, h2, h3, p {{
            color: {COLORS['text']};
        }}
        .stPlotlyChart {{
            background-color: {COLORS['card']};
            border-radius: 12px;
            padding: 10px;
        }}
    </style>
    """, unsafe_allow_html=True)
    
    # Dashboard Title
    st.markdown(f"""
    <div style="text-align: center; padding: 20px 0;">
        <h1 style="
            background: linear-gradient(90deg, {COLORS['primary']}, {COLORS['secondary']});
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-size: 48px;
            font-weight: 800;
            margin: 0;
        ">NYC Taxi Analytics</h1>
        <p style="color: {COLORS['muted']}; font-size: 18px;">
            Business Intelligence Dashboard • Gold Layer Analytics
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # =========================================================================
    # SECTION 1: EXECUTIVE OVERVIEW
    # =========================================================================
    section_header("", "Executive Overview", "At a glance: How is the business performing?")
    
    kpis = get_executive_kpis()
    
    if kpis:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            create_kpi_card(
                "Total Revenue",
                f"${kpis.get('total_revenue', 0):,.2f}",
                "All-time earnings"
            )
        
        with col2:
            create_kpi_card(
                "Total Trips",
                f"{int(kpis.get('total_trips', 0)):,}",
                "Completed rides"
            )
        
        with col3:
            create_kpi_card(
                "Avg Revenue/Trip",
                f"${kpis.get('avg_revenue_per_trip', 0):,.2f}",
                "Unit economics"
            )
        
        with col4:
            create_kpi_card(
                "Avg Trip Duration",
                f"{kpis.get('avg_duration', 0):.1f} min",
                "Average ride time"
            )
    
    # Rush Hour Comparison
    rush_df = get_rush_hour_comparison()
    if not rush_df.empty:
        st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        
        rush_data = rush_df[rush_df['is_rush_hour'] == True]
        off_data = rush_df[rush_df['is_rush_hour'] == False]
        
        with col1:
            rush_rev = rush_data['avg_revenue'].values[0] if not rush_data.empty else 0
            create_kpi_card(
                "Rush Hour Avg Revenue",
                f"${rush_rev:,.2f}",
                "7-9 AM & 4-6 PM"
            )
        
        with col2:
            off_rev = off_data['avg_revenue'].values[0] if not off_data.empty else 0
            create_kpi_card(
                "Off-Peak Avg Revenue",
                f"${off_rev:,.2f}",
                "Outside rush hours"
            )
    
    # =========================================================================
    # SECTION 2: REVENUE DEEP-DIVE
    # =========================================================================
    section_header("", "Revenue Deep-Dive", "Where does money come from?")
    
    col1, col2 = st.columns(2)
    
    # Revenue by Payment Type (Donut)
    with col1:
        payment_df = get_revenue_by_payment()
        if not payment_df.empty:
            fig = px.pie(
                payment_df,
                values='revenue',
                names='payment_type',
                hole=0.5,
                color='payment_type',
                color_discrete_map=PAYMENT_COLORS,
                title="Revenue by Payment Type"
            )
            fig.update_layout(
                paper_bgcolor='#1E293B',
                plot_bgcolor='#1E293B',
                font_color='#FFFFFF',
                title_font_color='#FFFFFF',
                legend=dict(
                    orientation="h", 
                    yanchor="bottom", 
                    y=-0.2,
                    font=dict(color='#FFFFFF')
                )
            )
            fig.update_traces(textfont_color='#FFFFFF')
            st.plotly_chart(fig, use_container_width=True)
    
    # Hourly Pattern (Bar Chart - better for limited data)
    with col2:
        hourly_df = get_hourly_pattern()
        if not hourly_df.empty:
            fig = px.bar(
                hourly_df,
                x='hour',
                y='revenue',
                title="Hourly Revenue Pattern",
                color_discrete_sequence=['#8B5CF6']  # Bright violet
            )
            fig.update_layout(
                paper_bgcolor='#1E293B',
                plot_bgcolor='#1E293B',
                font_color='#FFFFFF',
                title_font_color='#FFFFFF',
                xaxis_title="Hour of Day",
                yaxis_title="Revenue ($)",
                xaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF')),
                yaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF'))
            )
            fig.update_xaxes(gridcolor='rgba(255,255,255,0.2)', showgrid=True)
            fig.update_yaxes(gridcolor='rgba(255,255,255,0.2)', showgrid=True)
            st.plotly_chart(fig, use_container_width=True)
    
    # Top Routes (Horizontal Bar)
    routes_df = get_top_routes()
    if not routes_df.empty:
        routes_df['route'] = routes_df['pickup_zone'] + " → " + routes_df['dropoff_zone']
        fig = px.bar(
            routes_df.head(8),
            x='revenue',
            y='route',
            orientation='h',
            title="Top Revenue Routes",
            color='revenue',
            color_continuous_scale='Plasma'
        )
        fig.update_layout(
            paper_bgcolor='#1E293B',
            plot_bgcolor='#1E293B',
            font_color='#FFFFFF',
            title_font_color='#FFFFFF',
            showlegend=False,
            xaxis_title="Revenue ($)",
            yaxis_title="",
            xaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF')),
            yaxis=dict(categoryorder='total ascending', tickfont=dict(color='#FFFFFF'))
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # =========================================================================
    # SECTION 3: OPERATIONS & EFFICIENCY
    # =========================================================================
    section_header("", "Operations & Efficiency", "How efficient are our trips?")
    
    col1, col2 = st.columns(2)
    
    # Tip by Payment Type
    with col1:
        tip_df = get_tip_by_payment()
        if not tip_df.empty:
            fig = px.bar(
                tip_df,
                x='payment_type',
                y='avg_tip',
                title="Average Tip by Payment Type",
                color='payment_type',
                color_discrete_map=PAYMENT_COLORS
            )
            fig.update_layout(
                paper_bgcolor='#1E293B',
                plot_bgcolor='#1E293B',
                font_color='#FFFFFF',
                title_font_color='#FFFFFF',
                showlegend=False,
                xaxis_title="",
                yaxis_title="Average Tip ($)",
                xaxis=dict(tickfont=dict(color='#FFFFFF')),
                yaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF'))
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Duration Distribution
    with col2:
        duration_df = get_duration_distribution()
        if not duration_df.empty:
            # Sort by duration bucket order
            bucket_order = ['0-5 min', '5-10 min', '10-15 min', '15-20 min', '20-30 min', '30+ min']
            duration_df['duration_bucket'] = pd.Categorical(
                duration_df['duration_bucket'], 
                categories=bucket_order, 
                ordered=True
            )
            duration_df = duration_df.sort_values('duration_bucket')
            
            fig = px.bar(
                duration_df,
                x='duration_bucket',
                y='trips',
                title="Trip Duration Distribution",
                color_discrete_sequence=['#10B981']  # Emerald green
            )
            fig.update_layout(
                paper_bgcolor='#1E293B',
                plot_bgcolor='#1E293B',
                font_color='#FFFFFF',
                title_font_color='#FFFFFF',
                xaxis_title="Duration",
                yaxis_title="Number of Trips",
                xaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF')),
                yaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF'))
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # =========================================================================
    # SECTION 4: GEOGRAPHIC INTELLIGENCE
    # =========================================================================
    section_header("", "Geographic Intelligence", "Where are the hotspots?")
    
    col1, col2 = st.columns([2, 1])
    
    # Zone-to-Zone Heatmap
    with col1:
        heatmap_df = get_zone_heatmap()
        if not heatmap_df.empty:
            # Pivot for heatmap
            pivot_df = heatmap_df.pivot_table(
                values='trips',
                index='pickup_zone',
                columns='dropoff_zone',
                fill_value=0
            )
            
            fig = px.imshow(
                pivot_df.values,
                x=pivot_df.columns.tolist(),
                y=pivot_df.index.tolist(),
                title="Zone-to-Zone Trip Flow",
                color_continuous_scale='Plasma',
                aspect='auto'
            )
            fig.update_layout(
                paper_bgcolor='#1E293B',
                plot_bgcolor='#1E293B',
                font_color='#FFFFFF',
                title_font_color='#FFFFFF',
                xaxis_title="Dropoff Zone",
                yaxis_title="Pickup Zone",
                xaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF')),
                yaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF'))
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Zone Performance Summary
    with col2:
        zone_df = get_zone_performance()
        if not zone_df.empty:
            fig = px.bar(
                zone_df,
                x='total_revenue',
                y='zone',
                orientation='h',
                title="Revenue by Pickup Zone",
                color='zone',
                color_discrete_map=ZONE_COLORS
            )
            fig.update_layout(
                paper_bgcolor='#1E293B',
                plot_bgcolor='#1E293B',
                font_color='#FFFFFF',
                title_font_color='#FFFFFF',
                showlegend=False,
                xaxis_title="Revenue ($)",
                yaxis_title="",
                xaxis=dict(tickfont=dict(color='#FFFFFF'), title_font=dict(color='#FFFFFF')),
                yaxis=dict(categoryorder='total ascending', tickfont=dict(color='#FFFFFF'))
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Zone Performance Table
    zone_perf = get_zone_performance()
    if not zone_perf.empty:
        st.markdown(f"<h3 style='color: {COLORS['text']};'>Zone Performance Details</h3>", unsafe_allow_html=True)
        
        # Format the dataframe
        zone_perf_display = zone_perf.copy()
        zone_perf_display['total_revenue'] = zone_perf_display['total_revenue'].apply(lambda x: f"${x:,.2f}")
        zone_perf_display['avg_fare'] = zone_perf_display['avg_fare'].apply(lambda x: f"${x:,.2f}")
        zone_perf_display['avg_tip'] = zone_perf_display['avg_tip'].apply(lambda x: f"${x:,.2f}")
        zone_perf_display['avg_distance'] = zone_perf_display['avg_distance'].apply(lambda x: f"{x:.2f} mi")
        zone_perf_display.columns = ['Zone', 'Trips', 'Revenue', 'Avg Fare', 'Avg Tip', 'Avg Distance']
        
        st.dataframe(
            zone_perf_display,
            hide_index=True,
            use_container_width=True
        )
    
    # Footer
    st.markdown(f"""
    <div style="text-align: center; padding: 40px 0 20px 0; color: {COLORS['muted']};">
        <p>NYC Taxi Analytics Dashboard • Powered by Streamlit + Plotly</p>
        <p style="font-size: 12px;">Data source: Gold Layer (Apache Iceberg on MinIO)</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
