"""
Test Script for CDC Pipeline
This script tests the entire CDC flow: PostgreSQL -> Debezium -> Kafka
"""
import psycopg2
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import sys

# Configuration
DB_PARAMS = {
    'dbname': 'nyc_taxi_db',
    'user': 'admin',
    'password': '123',
    'host': 'localhost',
    'port': '5432'
}

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'taxi.public.taxi_trips'

def test_postgres_connection():
    """Test PostgreSQL connection"""
    print("\n" + "=" * 60)
    print("TEST 1: PostgreSQL Connection")
    print("=" * 60)
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("✓ Successfully connected to PostgreSQL")
        
        # Check if table exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'taxi_trips'
            """)
            if cur.fetchone()[0] == 1:
                print("✓ Table 'taxi_trips' exists")
                
                # Count records
                cur.execute("SELECT COUNT(*) FROM taxi_trips")
                count = cur.fetchone()[0]
                print(f"✓ Table contains {count} records")
            else:
                print("✗ Table 'taxi_trips' does not exist")
                conn.close()
                return False
        
        conn.close()
        return True
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}")
        return False

def test_kafka_connection():
    """Test Kafka connection"""
    print("\n" + "=" * 60)
    print("TEST 2: Kafka Connection")
    print("=" * 60)
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            consumer_timeout_ms=5000,
            auto_offset_reset='earliest'
        )
        
        # Get list of topics
        topics = consumer.topics()
        print(f"✓ Successfully connected to Kafka")
        print(f"✓ Available topics: {list(topics)}")
        
        if KAFKA_TOPIC in topics:
            print(f"✓ CDC topic '{KAFKA_TOPIC}' exists")
        else:
            print(f"⚠ CDC topic '{KAFKA_TOPIC}' does not exist yet")
            print("  This is normal if no data has been inserted yet")
        
        consumer.close()
        return True
    except Exception as e:
        print(f"✗ Kafka connection failed: {e}")
        return False

def insert_test_record():
    """Insert a test record into PostgreSQL"""
    print("\n" + "=" * 60)
    print("TEST 3: Insert Test Record")
    print("=" * 60)
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        test_data = {
            'pickup_time': datetime.now(),
            'dropoff_time': datetime.now(),
            'passenger_count': 2,
            'trip_distance': 5.5,
            'pickup_longitude': -73.98,
            'pickup_latitude': 40.75,
            'dropoff_longitude': -73.95,
            'dropoff_latitude': 40.78,
            'fare_amount': 15.50,
            'tip_amount': 3.10,
            'total_amount': 18.60
        }
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO taxi_trips (
                    tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count, trip_distance,
                    pickup_longitude, pickup_latitude,
                    dropoff_longitude, dropoff_latitude,
                    fare_amount, tip_amount, total_amount
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                test_data['pickup_time'], test_data['dropoff_time'],
                test_data['passenger_count'], test_data['trip_distance'],
                test_data['pickup_longitude'], test_data['pickup_latitude'],
                test_data['dropoff_longitude'], test_data['dropoff_latitude'],
                test_data['fare_amount'], test_data['tip_amount'], test_data['total_amount']
            ))
            record_id = cur.fetchone()[0]
            conn.commit()
            print(f"✓ Inserted test record with ID: {record_id}")
            print(f"  Fare: ${test_data['fare_amount']}, Distance: {test_data['trip_distance']} miles")
        
        conn.close()
        return record_id
    except Exception as e:
        print(f"✗ Failed to insert test record: {e}")
        return None

def consume_kafka_messages(timeout_seconds=30):
    """Consume messages from Kafka topic"""
    print("\n" + "=" * 60)
    print("TEST 4: Consume CDC Messages from Kafka")
    print("=" * 60)
    print(f"Listening to topic '{KAFKA_TOPIC}' for {timeout_seconds} seconds...")
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            consumer_timeout_ms=timeout_seconds * 1000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        message_count = 0
        start_time = time.time()
        
        for message in consumer:
            message_count += 1
            elapsed = time.time() - start_time
            
            print(f"\n--- Message {message_count} (at {elapsed:.1f}s) ---")
            print(f"Partition: {message.partition}, Offset: {message.offset}")
            
            data = message.value
            
            # Print a summary of the record
            if 'payload' in data:
                payload = data['payload']
                print(f"Operation: {payload.get('__op', 'N/A')}")
                print(f"Timestamp: {payload.get('__source_ts_ms', 'N/A')}")
                print(f"Record ID: {payload.get('id', 'N/A')}")
                print(f"Passenger Count: {payload.get('passenger_count', 'N/A')}")
                print(f"Trip Distance: {payload.get('trip_distance', 'N/A')}")
                print(f"Fare Amount: ${payload.get('fare_amount', 'N/A')}")
            else:
                print(f"Full message: {json.dumps(data, indent=2)}")
        
        consumer.close()
        
        if message_count > 0:
            print(f"\n✓ Successfully consumed {message_count} CDC messages from Kafka")
            return True
        else:
            print(f"\n⚠ No messages received in {timeout_seconds} seconds")
            print("  This might mean:")
            print("  1. No data has been inserted yet")
            print("  2. Debezium connector is not running")
            print("  3. Topic name is incorrect")
            return False
            
    except Exception as e:
        print(f"\n✗ Error consuming Kafka messages: {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("CDC PIPELINE TEST SUITE")
    print("Testing: PostgreSQL -> Debezium -> Kafka")
    print("=" * 60)
    
    # Test 1: PostgreSQL
    if not test_postgres_connection():
        print("\n✗ PostgreSQL test failed. Stopping tests.")
        sys.exit(1)
    
    # Test 2: Kafka
    if not test_kafka_connection():
        print("\n✗ Kafka test failed. Stopping tests.")
        sys.exit(1)
    
    # Test 3: Insert test record
    print("\nInserting a test record to trigger CDC...")
    record_id = insert_test_record()
    if not record_id:
        print("\n✗ Failed to insert test record. Stopping tests.")
        sys.exit(1)
    
    # Wait a bit for CDC to process
    print("\nWaiting 5 seconds for CDC to process...")
    time.sleep(5)
    
    # Test 4: Consume from Kafka
    success = consume_kafka_messages(timeout_seconds=30)
    
    # Final summary
    print("\n" + "=" * 60)
    if success:
        print("✓ ALL TESTS PASSED!")
        print("=" * 60)
        print("\nYour CDC pipeline is working correctly!")
        print("Changes to PostgreSQL are being captured and streamed to Kafka.")
        print("\nMonitoring URLs:")
        print("- Kafka UI: http://localhost:8080")
        print("- Debezium Connect: http://localhost:8083")
    else:
        print("⚠ TESTS COMPLETED WITH WARNINGS")
        print("=" * 60)
        print("\nPlease check:")
        print("1. Is the data-generator service running?")
        print("2. Is the Debezium connector registered? Run: python scripts/setup_debezium.py")
        print("3. Check Debezium logs: docker-compose logs debezium")

if __name__ == "__main__":
    main()
