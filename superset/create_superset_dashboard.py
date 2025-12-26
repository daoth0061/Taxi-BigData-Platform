"""
Superset Dashboard Creator via REST API

This script automatically creates datasets, charts, and a dashboard in Superset
using the REST API. Run this after setting up the database connection.

Prerequisites:
1. Superset running at http://localhost:8088
2. Database connection 'Taxi Gold Data' already created with connection string:
   hive://hive@spark-thrift:10000/default
"""

import requests
import json
import time

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

class SupersetAPIClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        self._login(username, password)
    
    def _login(self, username, password):
        """Authenticate and get access token"""
        # Get CSRF token first
        login_url = f"{self.base_url}/api/v1/security/login"
        payload = {
            "username": username,
            "password": password,
            "provider": "db",
            "refresh": True
        }
        
        response = self.session.post(login_url, json=payload)
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            self.session.headers.update({
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            })
            print("✅ Successfully authenticated with Superset")
            
            # Get CSRF token
            csrf_response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
            if csrf_response.status_code == 200:
                self.csrf_token = csrf_response.json().get("result")
                self.session.headers.update({"X-CSRFToken": self.csrf_token})
        else:
            raise Exception(f"Login failed: {response.text}")
    
    def get_databases(self):
        """Get list of databases"""
        response = self.session.get(f"{self.base_url}/api/v1/database/")
        return response.json().get("result", [])
    
    def get_database_id(self, database_name):
        """Get database ID by name"""
        databases = self.get_databases()
        for db in databases:
            if db.get("database_name") == database_name:
                return db.get("id")
        return None
    
    def create_database(self, database_name, sqlalchemy_uri):
        """Create a new database connection"""
        payload = {
            "database_name": database_name,
            "sqlalchemy_uri": sqlalchemy_uri,
            "expose_in_sqllab": True,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": True,
            "allow_run_async": True,
            "extra": json.dumps({
                "engine_params": {
                    "connect_args": {
                        "auth": "NONE"
                    }
                }
            })
        }
        response = self.session.post(f"{self.base_url}/api/v1/database/", json=payload)
        if response.status_code in [200, 201]:
            print(f"✅ Database '{database_name}' created")
            return response.json().get("id")
        else:
            print(f"⚠️ Database creation response: {response.status_code} - {response.text[:200]}")
            return None
    
    def get_datasets(self):
        """Get list of datasets"""
        response = self.session.get(f"{self.base_url}/api/v1/dataset/")
        return response.json().get("result", [])
    
    def create_dataset(self, database_id, table_name, schema="default"):
        """Create a new dataset"""
        payload = {
            "database": database_id,
            "table_name": table_name,
            "schema": schema
        }
        response = self.session.post(f"{self.base_url}/api/v1/dataset/", json=payload)
        if response.status_code in [200, 201]:
            dataset_id = response.json().get("id")
            print(f"✅ Dataset '{table_name}' created (ID: {dataset_id})")
            return dataset_id
        else:
            print(f"⚠️ Dataset '{table_name}' creation response: {response.status_code}")
            return None
    
    def get_dataset_id(self, table_name):
        """Get dataset ID by table name"""
        datasets = self.get_datasets()
        for ds in datasets:
            if ds.get("table_name") == table_name:
                return ds.get("id")
        return None
    
    def create_chart(self, chart_config, datasource_id, datasource_type="table"):
        """Create a new chart"""
        payload = {
            "slice_name": chart_config["slice_name"],
            "viz_type": chart_config["viz_type"],
            "description": chart_config.get("description", ""),
            "datasource_id": datasource_id,
            "datasource_type": datasource_type,
            "params": json.dumps(chart_config["params"]),
            "cache_timeout": chart_config.get("cache_timeout", 300)
        }
        response = self.session.post(f"{self.base_url}/api/v1/chart/", json=payload)
        if response.status_code in [200, 201]:
            chart_id = response.json().get("id")
            print(f"✅ Chart '{chart_config['slice_name']}' created (ID: {chart_id})")
            return chart_id
        else:
            print(f"⚠️ Chart '{chart_config['slice_name']}' creation failed: {response.status_code}")
            return None
    
    def create_dashboard(self, title, slug, chart_ids):
        """Create a new dashboard with charts"""
        # Create position JSON for layout
        position_json = self._generate_dashboard_layout(chart_ids)
        
        payload = {
            "dashboard_title": title,
            "slug": slug,
            "position_json": json.dumps(position_json),
            "published": True
        }
        response = self.session.post(f"{self.base_url}/api/v1/dashboard/", json=payload)
        if response.status_code in [200, 201]:
            dashboard_id = response.json().get("id")
            print(f"✅ Dashboard '{title}' created (ID: {dashboard_id})")
            return dashboard_id
        else:
            print(f"⚠️ Dashboard creation failed: {response.status_code} - {response.text[:200]}")
            return None
    
    def _generate_dashboard_layout(self, chart_ids):
        """Generate a grid layout for dashboard"""
        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
            "GRID_ID": {"children": [], "id": "GRID_ID", "parents": ["ROOT_ID"], "type": "GRID"}
        }
        
        row_idx = 0
        for i, chart_id in enumerate(chart_ids):
            if i % 3 == 0:  # 3 charts per row
                row_id = f"ROW-{row_idx}"
                position[row_id] = {
                    "children": [],
                    "id": row_id,
                    "meta": {"background": "BACKGROUND_TRANSPARENT"},
                    "parents": ["ROOT_ID", "GRID_ID"],
                    "type": "ROW"
                }
                position["GRID_ID"]["children"].append(row_id)
                row_idx += 1
            
            chart_holder_id = f"CHART-{chart_id}"
            current_row = f"ROW-{row_idx - 1}"
            position[chart_holder_id] = {
                "children": [],
                "id": chart_holder_id,
                "meta": {
                    "chartId": chart_id,
                    "height": 50,
                    "sliceName": f"Chart {chart_id}",
                    "width": 4
                },
                "parents": ["ROOT_ID", "GRID_ID", current_row],
                "type": "CHART"
            }
            position[current_row]["children"].append(chart_holder_id)
        
        return position


def main():
    print("="*60)
    print("SUPERSET DASHBOARD AUTOMATED CREATOR")
    print("="*60)
    print()
    
    # Initialize API client
    try:
        client = SupersetAPIClient(SUPERSET_URL, USERNAME, PASSWORD)
    except Exception as e:
        print(f"❌ Failed to connect to Superset: {e}")
        print("\nMake sure Superset is running at http://localhost:8088")
        return
    
    # Check/Create Database Connection
    print("\n📊 Setting up database connection...")
    database_name = "Taxi Gold Data"
    db_id = client.get_database_id(database_name)
    
    if not db_id:
        db_id = client.create_database(
            database_name,
            "hive://hive@spark-thrift:10000/default"
        )
        if not db_id:
            print("❌ Failed to create database connection")
            print("Please create it manually in Superset UI:")
            print("  - Go to Settings > Database Connections > + Database")
            print("  - Connection: hive://hive@spark-thrift:10000/default")
            return
    else:
        print(f"✅ Database '{database_name}' already exists (ID: {db_id})")
    
    # Create Datasets
    print("\n📁 Setting up datasets...")
    tables = ["gold_fact_trip", "gold_trip_analysis", "gold_dim_datetime", "gold_dim_zone"]
    dataset_ids = {}
    
    for table in tables:
        ds_id = client.get_dataset_id(table)
        if not ds_id:
            ds_id = client.create_dataset(db_id, table, "default")
        else:
            print(f"✅ Dataset '{table}' already exists (ID: {ds_id})")
        if ds_id:
            dataset_ids[table] = ds_id
        time.sleep(0.5)  # Rate limiting
    
    if not dataset_ids:
        print("❌ No datasets created. Please check database connection.")
        return
    
    # Define charts
    print("\n📈 Creating charts...")
    
    charts_config = [
        # KPI Cards (use gold_fact_trip)
        {
            "slice_name": "Total Trips",
            "viz_type": "big_number_total",
            "description": "Total number of taxi trips",
            "datasource": "gold_fact_trip",
            "params": {
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Total Trips"},
                "y_axis_format": ",.0f",
                "header_font_size": 0.4
            }
        },
        {
            "slice_name": "Total Revenue",
            "viz_type": "big_number_total", 
            "description": "Total revenue from all trips",
            "datasource": "gold_fact_trip",
            "params": {
                "metric": {"expressionType": "SQL", "sqlExpression": "SUM(total_amount)", "label": "Revenue"},
                "y_axis_format": "$,.2f",
                "header_font_size": 0.4
            }
        },
        {
            "slice_name": "Average Fare",
            "viz_type": "big_number_total",
            "description": "Average fare per trip",
            "datasource": "gold_fact_trip",
            "params": {
                "metric": {"expressionType": "SQL", "sqlExpression": "AVG(fare_amount)", "label": "Avg Fare"},
                "y_axis_format": "$,.2f"
            }
        },
        # Bar Charts (use gold_trip_analysis for joined data)
        {
            "slice_name": "Trips by Hour",
            "viz_type": "echarts_bar",
            "description": "Trip distribution by hour of day",
            "datasource": "gold_trip_analysis",
            "params": {
                "groupby": ["hour"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Trips"}],
                "row_limit": 24,
                "color_scheme": "supersetColors",
                "x_axis_label": "Hour",
                "y_axis_label": "Trips"
            }
        },
        {
            "slice_name": "Revenue by Zone",
            "viz_type": "echarts_bar",
            "description": "Total revenue by pickup zone",
            "datasource": "gold_trip_analysis",
            "params": {
                "groupby": ["pickup_zone"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "SUM(total_amount)", "label": "Revenue"}],
                "row_limit": 10,
                "order_desc": True,
                "color_scheme": "supersetColors"
            }
        },
        {
            "slice_name": "Trips by Day of Week",
            "viz_type": "echarts_bar",
            "description": "Trip distribution by day",
            "datasource": "gold_trip_analysis",
            "params": {
                "groupby": ["day_name"],
                "metrics": [{"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Trips"}],
                "row_limit": 7,
                "color_scheme": "bnbColors"
            }
        },
        # Pie Charts
        {
            "slice_name": "Payment Types",
            "viz_type": "pie",
            "description": "Distribution of payment methods",
            "datasource": "gold_fact_trip",
            "params": {
                "groupby": ["payment_type"],
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Count"},
                "donut": True,
                "show_labels": True,
                "label_type": "key_percent"
            }
        },
        {
            "slice_name": "Rush Hour Analysis",
            "viz_type": "pie",
            "description": "Rush hour vs non-rush hour trips",
            "datasource": "gold_trip_analysis",
            "params": {
                "groupby": ["is_rush_hour"],
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Count"},
                "show_labels": True
            }
        },
        # Tables
        {
            "slice_name": "Zone Flow Matrix",
            "viz_type": "table",
            "description": "Trips between zones",
            "datasource": "gold_trip_analysis",
            "params": {
                "groupby": ["pickup_zone", "dropoff_zone"],
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Trips"},
                    {"expressionType": "SQL", "sqlExpression": "SUM(total_amount)", "label": "Revenue"}
                ],
                "row_limit": 25,
                "order_desc": True,
                "include_search": True
            }
        },
        {
            "slice_name": "Hourly Summary",
            "viz_type": "table",
            "description": "Detailed metrics by hour",
            "datasource": "gold_trip_analysis",
            "params": {
                "groupby": ["hour"],
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Trips"},
                    {"expressionType": "SQL", "sqlExpression": "SUM(total_amount)", "label": "Revenue"},
                    {"expressionType": "SQL", "sqlExpression": "AVG(fare_amount)", "label": "Avg Fare"}
                ],
                "row_limit": 24,
                "order_desc": False
            }
        },
        # Heatmap
        {
            "slice_name": "Hour vs Day Heatmap",
            "viz_type": "heatmap",
            "description": "Trip intensity by hour and day",
            "datasource": "gold_trip_analysis",
            "params": {
                "all_columns_x": "hour",
                "all_columns_y": "day_name",
                "metric": {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Trips"},
                "linear_color_scheme": "blue_white_yellow",
                "show_legend": True
            }
        }
    ]
    
    chart_ids = []
    for chart_config in charts_config:
        datasource = chart_config.pop("datasource")
        ds_id = dataset_ids.get(datasource)
        if ds_id:
            chart_id = client.create_chart(chart_config, ds_id)
            if chart_id:
                chart_ids.append(chart_id)
        time.sleep(0.5)
    
    # Create Dashboard
    if chart_ids:
        print(f"\n🎨 Creating dashboard with {len(chart_ids)} charts...")
        dashboard_id = client.create_dashboard(
            "NYC Taxi Analytics",
            "nyc-taxi-analytics",
            chart_ids
        )
        
        if dashboard_id:
            print("\n" + "="*60)
            print("✅ DASHBOARD CREATION COMPLETE!")
            print("="*60)
            print(f"\n🔗 View your dashboard at:")
            print(f"   {SUPERSET_URL}/superset/dashboard/nyc-taxi-analytics/")
            print(f"\n📊 Charts created: {len(chart_ids)}")
            print(f"📁 Datasets configured: {len(dataset_ids)}")
    else:
        print("❌ No charts were created")
    
    # Export dashboard info
    export_data = {
        "dashboard_url": f"{SUPERSET_URL}/superset/dashboard/nyc-taxi-analytics/",
        "database_id": db_id,
        "datasets": dataset_ids,
        "chart_ids": chart_ids,
        "chart_count": len(chart_ids)
    }
    
    with open("superset_dashboard_info.json", "w") as f:
        json.dump(export_data, f, indent=2)
    print("\n📄 Dashboard info saved to: superset_dashboard_info.json")


if __name__ == "__main__":
    main()
