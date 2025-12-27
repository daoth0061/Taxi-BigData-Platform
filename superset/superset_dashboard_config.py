"""
Superset Dashboard Configuration Generator

This script creates a comprehensive dashboard configuration for taxi data analytics
that can be imported into Apache Superset.

Gold Schema Summary:
- dim_datetime: datetime_sk, dt_key, date_key, year, month, day, hour, day_of_week, day_name, is_rush_hour, is_weekend
- dim_zone: zone_id, zone_name (Manhattan, Brooklyn, Queens, Bronx, Staten Island, Unknown)
- fact_trip: pickup_datetime_sk, dropoff_datetime_sk, pickup_zone_id, dropoff_zone_id, 
             passenger_count, trip_distance, fare_amount, tip_amount, total_amount, 
             payment_type, trip_duration_minutes
"""

import json
import uuid
from datetime import datetime

# Generate unique IDs for dashboard elements
def gen_uuid():
    return str(uuid.uuid4())

# Dashboard metadata
DASHBOARD_TITLE = "NYC Taxi Analytics Dashboard"
DASHBOARD_SLUG = "nyc-taxi-analytics"
DATABASE_NAME = "Taxi Gold Data"

# Create chart configurations
charts = []

# ===== KPI CARDS =====

# 1. Total Trips KPI
charts.append({
    "slice_name": "Total Trips",
    "viz_type": "big_number_total",
    "description": "Total number of taxi trips in the dataset",
    "params": {
        "adhoc_filters": [],
        "datasource": "gold_fact_trip",
        "granularity_sqla": None,
        "header_font_size": 0.4,
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Total Trips"
        },
        "subheader_font_size": 0.15,
        "y_axis_format": ",.0f"
    },
    "cache_timeout": 300
})

# 2. Total Revenue KPI
charts.append({
    "slice_name": "Total Revenue",
    "viz_type": "big_number_total",
    "description": "Total revenue from all trips",
    "params": {
        "adhoc_filters": [],
        "datasource": "gold_fact_trip",
        "header_font_size": 0.4,
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "SUM(total_amount)",
            "label": "Total Revenue"
        },
        "subheader_font_size": 0.15,
        "y_axis_format": "$,.2f"
    },
    "cache_timeout": 300
})

# 3. Average Fare KPI
charts.append({
    "slice_name": "Average Fare",
    "viz_type": "big_number_total",
    "description": "Average fare per trip",
    "params": {
        "adhoc_filters": [],
        "datasource": "gold_fact_trip",
        "header_font_size": 0.4,
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "AVG(fare_amount)",
            "label": "Avg Fare"
        },
        "subheader_font_size": 0.15,
        "y_axis_format": "$,.2f"
    },
    "cache_timeout": 300
})

# 4. Average Trip Distance KPI
charts.append({
    "slice_name": "Avg Trip Distance",
    "viz_type": "big_number_total",
    "description": "Average distance per trip in miles",
    "params": {
        "adhoc_filters": [],
        "datasource": "gold_fact_trip",
        "header_font_size": 0.4,
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "AVG(trip_distance)",
            "label": "Avg Distance"
        },
        "subheader_font_size": 0.15,
        "y_axis_format": ",.2f"
    },
    "cache_timeout": 300
})

# 5. Average Tip Percentage KPI
charts.append({
    "slice_name": "Avg Tip %",
    "viz_type": "big_number_total",
    "description": "Average tip percentage of fare",
    "params": {
        "adhoc_filters": [],
        "datasource": "gold_fact_trip",
        "header_font_size": 0.4,
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount * 100 ELSE 0 END)",
            "label": "Avg Tip %"
        },
        "subheader_font_size": 0.15,
        "y_axis_format": ",.1f"
    },
    "cache_timeout": 300
})

# ===== BAR CHARTS =====

# 6. Trips by Hour of Day
charts.append({
    "slice_name": "Trips by Hour of Day",
    "viz_type": "echarts_bar",
    "description": "Distribution of trips across hours of the day",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["hour"],
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Trip Count"
        }],
        "order_desc": False,
        "row_limit": 24,
        "color_scheme": "supersetColors",
        "show_legend": False,
        "x_axis_label": "Hour of Day",
        "y_axis_label": "Number of Trips",
        "bar_stacked": False
    },
    "cache_timeout": 300
})

# 7. Trips by Day of Week
charts.append({
    "slice_name": "Trips by Day of Week",
    "viz_type": "echarts_bar",
    "description": "Distribution of trips across days of the week",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["day_name"],
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Trip Count"
        }],
        "order_desc": True,
        "row_limit": 7,
        "color_scheme": "supersetColors",
        "show_legend": False,
        "x_axis_label": "Day of Week",
        "y_axis_label": "Number of Trips"
    },
    "cache_timeout": 300
})

# 8. Revenue by Pickup Zone
charts.append({
    "slice_name": "Revenue by Pickup Zone",
    "viz_type": "echarts_bar",
    "description": "Total revenue grouped by pickup location",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["pickup_zone"],
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": "SUM(total_amount)",
            "label": "Total Revenue"
        }],
        "order_desc": True,
        "row_limit": 10,
        "color_scheme": "supersetColors",
        "show_legend": False,
        "x_axis_label": "Pickup Zone",
        "y_axis_label": "Revenue ($)",
        "y_axis_format": "$,.0f"
    },
    "cache_timeout": 300
})

# 9. Trips by Pickup Zone
charts.append({
    "slice_name": "Trips by Pickup Zone",
    "viz_type": "echarts_bar",
    "description": "Number of trips by pickup location",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["pickup_zone"],
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Trip Count"
        }],
        "order_desc": True,
        "row_limit": 10,
        "color_scheme": "bnbColors",
        "horizontal": True,
        "show_legend": False,
        "x_axis_label": "Number of Trips",
        "y_axis_label": "Zone"
    },
    "cache_timeout": 300
})

# ===== PIE CHARTS =====

# 10. Payment Type Distribution
charts.append({
    "slice_name": "Payment Type Distribution",
    "viz_type": "pie",
    "description": "Breakdown of trips by payment method",
    "params": {
        "datasource": "gold_fact_trip",
        "groupby": ["payment_type"],
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Count"
        },
        "color_scheme": "supersetColors",
        "show_legend": True,
        "show_labels": True,
        "label_type": "key_percent",
        "donut": True,
        "innerRadius": 40
    },
    "cache_timeout": 300
})

# 11. Rush Hour vs Non-Rush Hour
charts.append({
    "slice_name": "Rush Hour Distribution",
    "viz_type": "pie",
    "description": "Proportion of trips during rush hour vs non-rush hour",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["is_rush_hour"],
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Count"
        },
        "color_scheme": "googleCategory20c",
        "show_legend": True,
        "show_labels": True,
        "label_type": "key_percent",
        "donut": False
    },
    "cache_timeout": 300
})

# 12. Weekend vs Weekday
charts.append({
    "slice_name": "Weekend vs Weekday",
    "viz_type": "pie",
    "description": "Proportion of trips on weekends vs weekdays",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["is_weekend"],
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Count"
        },
        "color_scheme": "d3Category10",
        "show_legend": True,
        "show_labels": True,
        "label_type": "key_percent"
    },
    "cache_timeout": 300
})

# ===== LINE CHARTS =====

# 13. Revenue Trend by Date
charts.append({
    "slice_name": "Daily Revenue Trend",
    "viz_type": "echarts_timeseries_line",
    "description": "Revenue trend over time",
    "params": {
        "datasource": "gold_trip_analysis",
        "time_column": "date_key",
        "time_grain_sqla": "P1D",
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": "SUM(total_amount)",
            "label": "Daily Revenue"
        }],
        "color_scheme": "supersetColors",
        "show_legend": True,
        "x_axis_label": "Date",
        "y_axis_label": "Revenue ($)",
        "y_axis_format": "$,.0f",
        "show_markers": True,
        "markerSize": 6
    },
    "cache_timeout": 300
})

# 14. Trips Trend by Date
charts.append({
    "slice_name": "Daily Trip Volume",
    "viz_type": "echarts_timeseries_line",
    "description": "Number of trips over time",
    "params": {
        "datasource": "gold_trip_analysis",
        "time_column": "date_key",
        "time_grain_sqla": "P1D",
        "metrics": [{
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Trip Count"
        }],
        "color_scheme": "bnbColors",
        "show_legend": True,
        "x_axis_label": "Date",
        "y_axis_label": "Number of Trips",
        "show_markers": True
    },
    "cache_timeout": 300
})

# ===== TABLES =====

# 15. Zone to Zone Flow Table
charts.append({
    "slice_name": "Zone-to-Zone Trip Flow",
    "viz_type": "table",
    "description": "Top routes between pickup and dropoff zones",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["pickup_zone", "dropoff_zone"],
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Trips"
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "SUM(total_amount)",
                "label": "Revenue"
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(trip_distance)",
                "label": "Avg Distance"
            }
        ],
        "order_desc": True,
        "row_limit": 25,
        "table_timestamp_format": "%Y-%m-%d",
        "page_length": 10,
        "include_search": True,
        "show_cell_bars": True
    },
    "cache_timeout": 300
})

# 16. Hourly Performance Summary
charts.append({
    "slice_name": "Hourly Performance Summary",
    "viz_type": "table",
    "description": "Detailed metrics by hour of day",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["hour"],
        "metrics": [
            {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Trips"
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "SUM(total_amount)",
                "label": "Revenue"
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(fare_amount)",
                "label": "Avg Fare"
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(tip_amount)",
                "label": "Avg Tip"
            },
            {
                "expressionType": "SQL",
                "sqlExpression": "AVG(trip_distance)",
                "label": "Avg Distance"
            }
        ],
        "order_desc": False,
        "row_limit": 24,
        "include_search": False,
        "show_cell_bars": True
    },
    "cache_timeout": 300
})

# ===== HISTOGRAMS / DISTRIBUTIONS =====

# 17. Trip Distance Distribution
charts.append({
    "slice_name": "Trip Distance Distribution",
    "viz_type": "histogram",
    "description": "Distribution of trip distances",
    "params": {
        "datasource": "gold_fact_trip",
        "all_columns_x": ["trip_distance"],
        "link_length": 20,
        "color_scheme": "supersetColors",
        "x_axis_label": "Distance (miles)",
        "y_axis_label": "Frequency",
        "global_opacity": 0.8,
        "normalized": False
    },
    "cache_timeout": 300
})

# 18. Fare Amount Distribution
charts.append({
    "slice_name": "Fare Amount Distribution",
    "viz_type": "histogram",
    "description": "Distribution of fare amounts",
    "params": {
        "datasource": "gold_fact_trip",
        "all_columns_x": ["fare_amount"],
        "link_length": 25,
        "color_scheme": "bnbColors",
        "x_axis_label": "Fare ($)",
        "y_axis_label": "Frequency",
        "global_opacity": 0.8
    },
    "cache_timeout": 300
})

# ===== HEATMAP =====

# 19. Hour vs Day of Week Heatmap
charts.append({
    "slice_name": "Trips Heatmap: Hour vs Day",
    "viz_type": "heatmap",
    "description": "Intensity of trips by hour and day of week",
    "params": {
        "datasource": "gold_trip_analysis",
        "all_columns_x": "hour",
        "all_columns_y": "day_name",
        "metric": {
            "expressionType": "SQL",
            "sqlExpression": "COUNT(*)",
            "label": "Trip Count"
        },
        "linear_color_scheme": "blue_white_yellow",
        "xscale_interval": 1,
        "yscale_interval": 1,
        "canvas_image_rendering": "auto",
        "normalize_across": None,
        "show_legend": True,
        "show_values": True
    },
    "cache_timeout": 300
})

# ===== BOX PLOT =====

# 20. Fare by Zone Box Plot
charts.append({
    "slice_name": "Fare Distribution by Zone",
    "viz_type": "box_plot",
    "description": "Statistical distribution of fares across zones",
    "params": {
        "datasource": "gold_trip_analysis",
        "groupby": ["pickup_zone"],
        "metrics": [{
            "expressionType": "SIMPLE",
            "column": {"column_name": "fare_amount"},
            "aggregate": "MEAN"
        }],
        "whisker_options": "Min/max (no outliers)",
        "color_scheme": "supersetColors"
    },
    "cache_timeout": 300
})

# ===== SCATTER PLOT =====

# 21. Distance vs Fare Scatter
charts.append({
    "slice_name": "Distance vs Fare",
    "viz_type": "echarts_scatter",
    "description": "Relationship between trip distance and fare",
    "params": {
        "datasource": "gold_fact_trip",
        "x": {
            "expressionType": "SIMPLE",
            "column": {"column_name": "trip_distance"},
            "aggregate": None
        },
        "y": {
            "expressionType": "SIMPLE", 
            "column": {"column_name": "fare_amount"},
            "aggregate": None
        },
        "size": {
            "expressionType": "SIMPLE",
            "column": {"column_name": "tip_amount"},
            "aggregate": None
        },
        "color_scheme": "supersetColors",
        "x_axis_label": "Distance (miles)",
        "y_axis_label": "Fare ($)",
        "row_limit": 1000
    },
    "cache_timeout": 300
})

# ===== DASHBOARD LAYOUT =====

# Create dashboard layout with grid positions
dashboard_layout = {
    "CHART_HOLDER": [],
    "rows": [
        {
            "row_id": "row_kpis",
            "title": "Key Performance Indicators",
            "charts": ["Total Trips", "Total Revenue", "Average Fare", "Avg Trip Distance", "Avg Tip %"]
        },
        {
            "row_id": "row_temporal",
            "title": "Temporal Analysis",
            "charts": ["Trips by Hour of Day", "Trips by Day of Week", "Daily Revenue Trend"]
        },
        {
            "row_id": "row_geographic",
            "title": "Geographic Insights",
            "charts": ["Revenue by Pickup Zone", "Trips by Pickup Zone", "Zone-to-Zone Trip Flow"]
        },
        {
            "row_id": "row_distributions",
            "title": "Payment & Period Distribution",
            "charts": ["Payment Type Distribution", "Rush Hour Distribution", "Weekend vs Weekday"]
        },
        {
            "row_id": "row_trends",
            "title": "Volume & Value Trends",
            "charts": ["Daily Trip Volume", "Trips Heatmap: Hour vs Day"]
        },
        {
            "row_id": "row_stats",
            "title": "Statistical Analysis",
            "charts": ["Trip Distance Distribution", "Fare Amount Distribution", "Distance vs Fare"]
        },
        {
            "row_id": "row_details",
            "title": "Detailed Analysis",
            "charts": ["Hourly Performance Summary", "Fare Distribution by Zone"]
        }
    ]
}

# Create the complete dashboard export
dashboard_export = {
    "dashboard_title": DASHBOARD_TITLE,
    "slug": DASHBOARD_SLUG,
    "position_json": json.dumps(dashboard_layout),
    "metadata": {
        "color_scheme": "supersetColors",
        "refresh_frequency": 300,
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "label_colors": {},
        "shared_label_colors": {},
        "color_scheme_domain": [],
        "cross_filters_enabled": True,
        "native_filter_configuration": []
    },
    "charts": charts,
    "datasources": [
        {
            "table_name": "gold_fact_trip",
            "schema": "default",
            "database": DATABASE_NAME,
            "sql": None,
            "columns": [
                {"column_name": "pickup_datetime_sk", "type": "BIGINT"},
                {"column_name": "dropoff_datetime_sk", "type": "BIGINT"},
                {"column_name": "pickup_zone_id", "type": "INT"},
                {"column_name": "dropoff_zone_id", "type": "INT"},
                {"column_name": "passenger_count", "type": "INT"},
                {"column_name": "trip_distance", "type": "FLOAT"},
                {"column_name": "fare_amount", "type": "FLOAT"},
                {"column_name": "tip_amount", "type": "FLOAT"},
                {"column_name": "total_amount", "type": "FLOAT"},
                {"column_name": "payment_type", "type": "STRING"},
                {"column_name": "trip_duration_minutes", "type": "FLOAT"}
            ],
            "metrics": [
                {"metric_name": "count", "expression": "COUNT(*)"},
                {"metric_name": "total_revenue", "expression": "SUM(total_amount)"},
                {"metric_name": "avg_fare", "expression": "AVG(fare_amount)"},
                {"metric_name": "avg_tip", "expression": "AVG(tip_amount)"},
                {"metric_name": "avg_distance", "expression": "AVG(trip_distance)"}
            ]
        },
        {
            "table_name": "gold_trip_analysis",
            "schema": "default",
            "database": DATABASE_NAME,
            "sql": """
                SELECT 
                    f.*, 
                    d.dt_key, d.date_key, d.year, d.month, d.day, d.hour, 
                    d.day_of_week, d.day_name, d.is_rush_hour, d.is_weekend,
                    pz.zone_name as pickup_zone,
                    dz.zone_name as dropoff_zone
                FROM gold_fact_trip f
                JOIN gold_dim_datetime d ON f.pickup_datetime_sk = d.datetime_sk
                JOIN gold_dim_zone pz ON f.pickup_zone_id = pz.zone_id
                JOIN gold_dim_zone dz ON f.dropoff_zone_id = dz.zone_id
            """,
            "columns": [
                {"column_name": "pickup_datetime_sk", "type": "BIGINT"},
                {"column_name": "dropoff_datetime_sk", "type": "BIGINT"},
                {"column_name": "pickup_zone_id", "type": "INT"},
                {"column_name": "dropoff_zone_id", "type": "INT"},
                {"column_name": "passenger_count", "type": "INT"},
                {"column_name": "trip_distance", "type": "FLOAT"},
                {"column_name": "fare_amount", "type": "FLOAT"},
                {"column_name": "tip_amount", "type": "FLOAT"},
                {"column_name": "total_amount", "type": "FLOAT"},
                {"column_name": "payment_type", "type": "STRING"},
                {"column_name": "trip_duration_minutes", "type": "FLOAT"},
                {"column_name": "dt_key", "type": "TIMESTAMP", "is_dttm": True},
                {"column_name": "date_key", "type": "DATE", "is_dttm": True},
                {"column_name": "year", "type": "INT", "groupby": True},
                {"column_name": "month", "type": "INT", "groupby": True},
                {"column_name": "day", "type": "INT", "groupby": True},
                {"column_name": "hour", "type": "INT", "groupby": True},
                {"column_name": "day_of_week", "type": "INT", "groupby": True},
                {"column_name": "day_name", "type": "STRING", "groupby": True},
                {"column_name": "is_rush_hour", "type": "BOOLEAN", "groupby": True},
                {"column_name": "is_weekend", "type": "BOOLEAN", "groupby": True},
                {"column_name": "pickup_zone", "type": "STRING", "groupby": True},
                {"column_name": "dropoff_zone", "type": "STRING", "groupby": True}
            ],
            "metrics": [
                {"metric_name": "count", "expression": "COUNT(*)"},
                {"metric_name": "total_revenue", "expression": "SUM(total_amount)"},
                {"metric_name": "avg_fare", "expression": "AVG(fare_amount)"},
                {"metric_name": "avg_tip", "expression": "AVG(tip_amount)"},
                {"metric_name": "avg_distance", "expression": "AVG(trip_distance)"},
                {"metric_name": "total_passengers", "expression": "SUM(passenger_count)"}
            ]
        },
        {
            "table_name": "gold_dim_datetime",
            "schema": "default",
            "database": DATABASE_NAME,
            "columns": [
                {"column_name": "datetime_sk", "type": "BIGINT"},
                {"column_name": "dt_key", "type": "TIMESTAMP", "is_dttm": True},
                {"column_name": "date_key", "type": "DATE", "is_dttm": True},
                {"column_name": "year", "type": "INT"},
                {"column_name": "month", "type": "INT"},
                {"column_name": "day", "type": "INT"},
                {"column_name": "hour", "type": "INT"},
                {"column_name": "day_of_week", "type": "INT"},
                {"column_name": "day_name", "type": "STRING"},
                {"column_name": "is_rush_hour", "type": "BOOLEAN"},
                {"column_name": "is_weekend", "type": "BOOLEAN"}
            ]
        },
        {
            "table_name": "gold_dim_zone",
            "schema": "default",
            "database": DATABASE_NAME,
            "columns": [
                {"column_name": "zone_id", "type": "INT"},
                {"column_name": "zone_name", "type": "STRING"},
                {"column_name": "lat_min", "type": "DOUBLE"},
                {"column_name": "lat_max", "type": "DOUBLE"},
                {"column_name": "lon_min", "type": "DOUBLE"},
                {"column_name": "lon_max", "type": "DOUBLE"}
            ]
        }
    ],
    "filters": [
        {
            "filter_name": "Pickup Zone Filter",
            "filter_type": "filter_select",
            "column": "pickup_zone",
            "datasource": "gold_trip_analysis",
            "multiple": True
        },
        {
            "filter_name": "Payment Type Filter",
            "filter_type": "filter_select",
            "column": "payment_type",
            "datasource": "gold_fact_trip",
            "multiple": True
        },
        {
            "filter_name": "Date Range Filter",
            "filter_type": "filter_time",
            "column": "date_key",
            "datasource": "gold_trip_analysis"
        },
        {
            "filter_name": "Rush Hour Toggle",
            "filter_type": "filter_select",
            "column": "is_rush_hour",
            "datasource": "gold_trip_analysis"
        }
    ],
    "export_info": {
        "exported_at": datetime.now().isoformat(),
        "version": "1.0.0",
        "schema_summary": {
            "dim_datetime": {
                "rows": 3178,
                "columns": ["datetime_sk", "dt_key", "date_key", "year", "month", "day", "hour", "day_of_week", "day_name", "is_rush_hour", "is_weekend"]
            },
            "dim_zone": {
                "rows": 6,
                "zones": ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "Unknown"]
            },
            "fact_trip": {
                "rows": 5663,
                "columns": ["pickup_datetime_sk", "dropoff_datetime_sk", "pickup_zone_id", "dropoff_zone_id", "passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount", "payment_type", "trip_duration_minutes"]
            }
        }
    }
}

# Save to JSON file
output_file = "superset_dashboard_export.json"
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(dashboard_export, f, indent=2, ensure_ascii=False)

print(f"Dashboard configuration saved to: {output_file}")
print(f"\nDashboard: {DASHBOARD_TITLE}")
print(f"Total Charts: {len(charts)}")
print("\nCharts included:")
for i, chart in enumerate(charts, 1):
    print(f"  {i:2d}. {chart['slice_name']:<30} ({chart['viz_type']})")

print("\n" + "="*60)
print("NEXT STEPS TO IMPORT INTO SUPERSET:")
print("="*60)
print("""
1. Open Superset at http://localhost:8088 (admin/admin)

2. First, create the database connection:
   - Go to Settings > Database Connections > + Database
   - Select 'Other' or 'Apache Hive'
   - Connection string: hive://hive@spark-thrift:10000/default
   - Name: 'Taxi Gold Data'
   - Test and Save

3. Add datasets (Data > Datasets > + Dataset):
   - Select database 'Taxi Gold Data'
   - Schema: default
   - Add tables: gold_dim_datetime, gold_dim_zone, gold_fact_trip, gold_trip_analysis

4. Create charts manually using this JSON as reference:
   - Go to Charts > + Chart
   - Select dataset and chart type
   - Configure using params from this JSON

5. Or use Superset CLI to import (if available):
   superset import-dashboards --path superset_dashboard_export.json
""")
