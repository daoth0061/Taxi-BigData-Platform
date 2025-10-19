"""
Setup Script for Debezium Connector
This script registers the Debezium PostgreSQL connector with Kafka Connect
"""
import requests
import json
import time
import sys

DEBEZIUM_URL = "http://localhost:8083"
CONNECTOR_CONFIG_FILE = "configs/debezium-connector.json"

def wait_for_debezium(max_retries=30, delay=2):
    """Wait for Debezium Connect to be ready"""
    print("Waiting for Debezium Connect to be ready...")
    for i in range(max_retries):
        try:
            response = requests.get(f"{DEBEZIUM_URL}/")
            if response.status_code == 200:
                print("✓ Debezium Connect is ready!")
                return True
        except requests.exceptions.ConnectionError:
            pass
        
        print(f"  Attempt {i+1}/{max_retries} - Debezium not ready yet...")
        time.sleep(delay)
    
    print("✗ Debezium Connect did not become ready in time")
    return False

def check_existing_connectors():
    """Check if connector already exists"""
    try:
        response = requests.get(f"{DEBEZIUM_URL}/connectors")
        if response.status_code == 200:
            connectors = response.json()
            print(f"Existing connectors: {connectors}")
            return connectors
        return []
    except Exception as e:
        print(f"Error checking connectors: {e}")
        return []

def delete_connector(connector_name):
    """Delete existing connector"""
    try:
        response = requests.delete(f"{DEBEZIUM_URL}/connectors/{connector_name}")
        if response.status_code == 204:
            print(f"✓ Deleted existing connector: {connector_name}")
            time.sleep(2)
            return True
        return False
    except Exception as e:
        print(f"Error deleting connector: {e}")
        return False

def register_connector():
    """Register the Debezium PostgreSQL connector"""
    print("\nRegistering Debezium PostgreSQL connector...")
    
    # Load connector configuration
    with open(CONNECTOR_CONFIG_FILE, 'r') as f:
        connector_config = json.load(f)
    
    connector_name = connector_config['name']
    
    # Check if connector already exists
    existing = check_existing_connectors()
    if connector_name in existing:
        print(f"Connector '{connector_name}' already exists. Deleting it first...")
        delete_connector(connector_name)
    
    # Register the connector
    try:
        response = requests.post(
            f"{DEBEZIUM_URL}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config)
        )
        
        if response.status_code in [200, 201]:
            print(f"✓ Successfully registered connector: {connector_name}")
            print(f"Response: {json.dumps(response.json(), indent=2)}")
            return True
        else:
            print(f"✗ Failed to register connector. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Error registering connector: {e}")
        return False

def check_connector_status(connector_name="taxi-postgres-connector"):
    """Check the status of the connector"""
    print(f"\nChecking connector status...")
    try:
        response = requests.get(f"{DEBEZIUM_URL}/connectors/{connector_name}/status")
        if response.status_code == 200:
            status = response.json()
            print(f"Connector Status:")
            print(json.dumps(status, indent=2))
            
            connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
            if connector_state == 'RUNNING':
                print(f"✓ Connector is RUNNING")
                return True
            else:
                print(f"✗ Connector state: {connector_state}")
                return False
        else:
            print(f"✗ Failed to get connector status. Status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Error checking connector status: {e}")
        return False

def main():
    print("=" * 60)
    print("Debezium PostgreSQL Connector Setup")
    print("=" * 60)
    
    # Wait for Debezium to be ready
    if not wait_for_debezium():
        print("\n✗ Setup failed: Debezium Connect is not available")
        sys.exit(1)
    
    # Register the connector
    if not register_connector():
        print("\n✗ Setup failed: Could not register connector")
        sys.exit(1)
    
    # Wait a bit for connector to initialize
    print("\nWaiting for connector to initialize...")
    time.sleep(5)
    
    # Check connector status
    if check_connector_status():
        print("\n" + "=" * 60)
        print("✓ Setup completed successfully!")
        print("=" * 60)
        print("\nThe Debezium connector is now capturing changes from PostgreSQL")
        print("and streaming them to Kafka topic: 'taxi.public.taxi_trips'")
        print("\nNext steps:")
        print("1. Monitor Kafka topics at: http://localhost:8080")
        print("2. Run the test script: python scripts/test_cdc.py")
    else:
        print("\n✗ Setup completed with warnings")
        print("Please check the connector configuration")
        sys.exit(1)

if __name__ == "__main__":
    main()
