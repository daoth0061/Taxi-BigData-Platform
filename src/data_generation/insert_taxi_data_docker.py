import psycopg2
from datetime import datetime, timedelta
import random
import os
import time

# Database connection parameters - read from environment variables
DB_PARAMS = {
    'dbname': os.getenv('DB_NAME', 'nyc_taxi_db'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', '123'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

# Create table SQL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS taxi_trips (
    id SERIAL PRIMARY KEY,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT
);
"""

def create_connection():
    """Create a database connection with retry logic"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to database at {DB_PARAMS['host']}:{DB_PARAMS['port']}...")
            conn = psycopg2.connect(**DB_PARAMS)
            print("Successfully connected to database!")
            return conn
        except psycopg2.Error as e:
            print(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Max retries reached. Could not connect to database.")
                raise

def create_table(conn):
    """Create the taxi_trips table if it doesn't exist"""
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()
            print("Table 'taxi_trips' is ready.")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise

def generate_random_location():
    """Generate random NYC-like coordinates"""
    # Approximate NYC bounds
    return (
        random.uniform(-74.03, -73.75),  # longitude
        random.uniform(40.63, 40.85)      # latitude
    )

def generate_trip_data(num_records=1000):
    """Generate simulated taxi trip data"""
    for _ in range(num_records):
        pickup_time = datetime.now() - timedelta(days=random.randint(0, 30))
        # Drop-off time between pickup + 10 minutes and pickup + 120 minutes
        dropoff_time = pickup_time + timedelta(minutes=random.randint(10, 120))
        
        pickup_long, pickup_lat = generate_random_location()
        dropoff_long, dropoff_lat = generate_random_location()
        
        # Calculate a reasonable fare based on time and distance
        trip_distance = random.uniform(0.5, 20.0)
        fare_amount = 2.50 + (trip_distance * 2.50)  # Base fare + distance rate
        tip_amount = fare_amount * random.uniform(0, 0.3)  # 0-30% tip
        total_amount = fare_amount + tip_amount

        yield (
            pickup_time,
            dropoff_time,
            random.randint(1, 6),          # passenger_count
            round(trip_distance, 2),
            pickup_long,
            pickup_lat,
            dropoff_long,
            dropoff_lat,
            round(fare_amount, 2),
            round(tip_amount, 2),
            round(total_amount, 2)
        )

def insert_data(conn, num_records=1000):
    """Insert simulated data into the database"""
    insert_sql = """
    INSERT INTO taxi_trips (
        tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance,
        pickup_longitude, pickup_latitude,
        dropoff_longitude, dropoff_latitude,
        fare_amount, tip_amount, total_amount
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        with conn.cursor() as cur:
            for record in generate_trip_data(num_records):
                cur.execute(insert_sql, record)
            conn.commit()
            print(f"Successfully inserted {num_records} records at {datetime.now()}")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        raise

def generate_continous_data(interval_seconds=5, batch_size=10):
    """Continuously generate and insert data at specified intervals"""
    conn = create_connection()
    
    # Create table first
    create_table(conn)
    
    try:
        print(f"Starting continuous data generation: {batch_size} records every {interval_seconds} seconds")
        while True:
            insert_data(conn, batch_size)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        conn.close()
        print("Data generation stopped by user.")
    except Exception as e:
        print(f"An error occurred during continuous data generation: {e}")
        if conn:
            conn.rollback()
        raise

if __name__ == "__main__":
    interval = int(os.getenv('INTERVAL_SECONDS', 5))
    batch = int(os.getenv('BATCH_SIZE', 10))
    generate_continous_data(interval_seconds=interval, batch_size=batch)
