"""
Create Superset datasets via API
Run this after connecting Hive database in Superset UI
"""
import requests
import json

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

def get_access_token():
    """Login and get access token"""
    session = requests.Session()
    login_url = f"{SUPERSET_URL}/api/v1/security/login"
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True
    }
    response = session.post(login_url, json=payload)
    if response.status_code == 200:
        token = response.json()["access_token"]
        return session, token
    else:
        print(f"Login failed: {response.text}")
        return None, None

def get_csrf_token(session, headers):
    """Get CSRF token"""
    csrf_url = f"{SUPERSET_URL}/api/v1/security/csrf_token/"
    response = session.get(csrf_url, headers=headers)
    if response.status_code == 200:
        return response.json()["result"]
    print(f"Failed to get CSRF token: {response.text}")
    return None

def get_database_id(session, headers, db_name="Hive"):
    """Get the Hive database ID"""
    url = f"{SUPERSET_URL}/api/v1/database/"
    response = session.get(url, headers=headers)
    if response.status_code == 200:
        databases = response.json()["result"]
        for db in databases:
            if db_name.lower() in db["database_name"].lower():
                print(f"Found database: {db['database_name']} (ID: {db['id']})")
                return db["id"]
    print(f"Database '{db_name}' not found. Available databases:")
    if response.status_code == 200:
        for db in response.json()["result"]:
            print(f"  - {db['database_name']}")
    return None

def create_dataset(session, headers, database_id, table_name, schema="default"):
    """Create a dataset for a table"""
    url = f"{SUPERSET_URL}/api/v1/dataset/"
    
    payload = {
        "database": database_id,
        "schema": schema,
        "table_name": table_name
    }
    
    response = session.post(url, headers=headers, json=payload)
    if response.status_code == 201:
        print(f"✓ Created dataset: {table_name}")
        return response.json()
    else:
        print(f"✗ Failed to create {table_name}: {response.status_code} - {response.text}")
        return None

def main():
    print("=== Creating Superset Datasets ===\n")
    
    # Get access token
    session, token = get_access_token()
    if not token:
        print("Failed to get access token. Make sure Superset is running.")
        return
    
    print("✓ Logged in to Superset\n")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Get CSRF token
    csrf_token = get_csrf_token(session, headers)
    if csrf_token:
        headers["X-CSRFToken"] = csrf_token
        print("✓ Got CSRF token\n")
    
    # Get database ID
    db_id = get_database_id(session, headers)
    if not db_id:
        print("\nPlease create a Hive database connection first in Superset UI:")
        print("  1. Go to Settings → Database Connections → + Database")
        print("  2. Select 'Apache Hive'")
        print("  3. SQLAlchemy URI: hive://hive@spark-thrift:10000/default")
        print("  4. Click 'Test Connection' then 'Connect'")
        return
    
    # Create datasets for gold tables
    tables = ["gold_dim_datetime", "gold_dim_zone", "gold_fact_trip"]
    
    print(f"\nCreating datasets for {len(tables)} tables...")
    for table in tables:
        create_dataset(session, headers, db_id, table)
    
    print("\n=== Done! ===")
    print("Go to Superset → Charts → + Chart to create visualizations")

if __name__ == "__main__":
    main()
