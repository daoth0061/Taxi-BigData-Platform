import os

import psycopg2
from datetime import datetime, timedelta
import random

# Database connection parameters
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "nyc_taxi_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "123"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
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
    total_amount FLOAT,
    payment_type INTEGER
);
"""

def create_connection():
    """Create a database connection"""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        raise

def create_table(conn):
    """Create the taxi_trips table if it doesn't exist"""
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()
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

def generate_trip_data_past(num_records=10000):
    """Generate simulated taxi trip data"""

    PAYMENT_TYPES = [0, 1, 2, 3, 4, 5, 6]
    for _ in range(num_records):
        pickup_time = datetime.now() - timedelta(days=random.randint(0, 30))
        dropoff_time = pickup_time + timedelta(minutes=random.randint(10, 120))
        
        pickup_long, pickup_lat = generate_random_location()
        dropoff_long, dropoff_lat = generate_random_location()
        
        trip_distance = random.uniform(0.5, 20.0)
        fare_amount = 2.50 + (trip_distance * 2.50)
        tip_amount = fare_amount * random.uniform(0, 0.3)
        total_amount = fare_amount + tip_amount
        
        payment_type = random.choice(PAYMENT_TYPES)

        yield (
            pickup_time,
            dropoff_time,
            random.randint(1, 6), # passenger_count
            round(trip_distance, 2),
            pickup_long,
            pickup_lat,
            dropoff_long,
            dropoff_lat,
            round(fare_amount, 2),
            round(tip_amount, 2),
            round(total_amount, 2),
            payment_type
        )



def generate_trip_data(num_records=2000):
    """Generate simulated taxi trip data with realistic NYC patterns"""
    
    # Payment types: 1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown
    # Real NYC data: ~70% Credit, ~25% Cash, ~5% other
    PAYMENT_WEIGHTS = [0.70, 0.25, 0.02, 0.01, 0.02]
    PAYMENT_TYPES = [1, 2, 3, 4, 5]
    
    # Hour weights - morning rush 7-9, evening rush 17-19
    HOUR_WEIGHTS = [
        0.01, 0.01, 0.01, 0.01, 0.02, 0.03,  # 0-5 AM (very low)
        0.05, 0.08, 0.09, 0.07, 0.06, 0.06,  # 6-11 AM (morning rush peaks at 8)
        0.06, 0.06, 0.06, 0.06, 0.07, 0.09,  # 12-17 PM (afternoon, evening rush starts)
        0.08, 0.06, 0.05, 0.04, 0.03, 0.02   # 18-23 PM (decline at night)
    ]
    
    # Passenger count: most trips are 1-2 passengers
    PASSENGER_WEIGHTS = [0.70, 0.15, 0.08, 0.04, 0.02, 0.01]
    PASSENGER_COUNTS = [1, 2, 3, 4, 5, 6]
    
    for _ in range(num_records):
        # Generate pickup time with hour distribution
        base_time = datetime.now()
        # Random day in past 30 days
        days_ago = random.randint(0, 30)
        # Weighted hour selection
        hour = random.choices(range(24), weights=HOUR_WEIGHTS)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        pickup_time = base_time.replace(
            hour=hour, minute=minute, second=second
        ) - timedelta(days=days_ago)
        
        # Trip distance with realistic distribution (most trips 1-5 miles)
        if random.random() < 0.6:
            trip_distance = random.gauss(3.0, 1.5)  # Short trips peak at 3 miles
        elif random.random() < 0.9:
            trip_distance = random.gauss(8.0, 3.0)  # Medium trips
        else:
            trip_distance = random.gauss(15.0, 5.0)  # Long trips (airport, etc)
        trip_distance = max(0.5, min(30.0, trip_distance))
        
        # Trip duration correlated with distance (roughly 2min/mile + base + variability)
        base_duration = 5  # Base 5 min for pickup
        per_mile_time = random.gauss(4, 1)  # ~4 min per mile on average
        trip_duration = base_duration + (trip_distance * per_mile_time) + random.gauss(0, 3)
        trip_duration = max(5, min(120, trip_duration))
        dropoff_time = pickup_time + timedelta(minutes=trip_duration)
        
        # Calculate fare (NYC: $2.50 base + $2.50/mile + time)
        fare_amount = 2.50 + (trip_distance * 2.50) + (trip_duration * 0.50)
        # Add surge pricing randomly (10% chance, 1.5-2x multiplier)
        if random.random() < 0.10:
            fare_amount *= random.uniform(1.5, 2.0)
        
        # Payment type with realistic weights
        payment_type = random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS)[0]
        
        # Tip amount depends on payment type
        if payment_type == 1:  # Credit card - tips are common
            tip_pct = random.gauss(0.18, 0.05)  # ~18% average tip
            tip_pct = max(0, min(0.30, tip_pct))
            tip_amount = fare_amount * tip_pct
        elif payment_type == 2:  # Cash - less tipping tracked
            tip_amount = 0 if random.random() < 0.7 else fare_amount * random.uniform(0.05, 0.15)
        else:  # Other - no tip
            tip_amount = 0
        
        total_amount = fare_amount + tip_amount
        
        # Passenger count with realistic weights
        passenger_count = random.choices(PASSENGER_COUNTS, weights=PASSENGER_WEIGHTS)[0]
        
        pickup_long, pickup_lat = generate_random_location()
        dropoff_long, dropoff_lat = generate_random_location()

        yield (
            pickup_time,
            dropoff_time,
            passenger_count,
            round(trip_distance, 2),
            pickup_long,
            pickup_lat,
            dropoff_long,
            dropoff_lat,
            round(fare_amount, 2),
            round(tip_amount, 2),
            round(total_amount, 2),
            payment_type
        )

def insert_data_past(conn, num_records=1000):
    """Insert simulated past data into the database"""
    insert_sql = """
    INSERT INTO taxi_trips (
        tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance,
        pickup_longitude, pickup_latitude,
        dropoff_longitude, dropoff_latitude,
        fare_amount, tip_amount, total_amount, payment_type
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    try:
        with conn.cursor() as cur:
            for record in generate_trip_data_past(num_records):
                cur.execute(insert_sql, record)
            conn.commit()
            print(f"Successfully inserted past {num_records} records")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        raise


def insert_data(conn, num_records=1000):
    """Insert simulated data into the database"""
    insert_sql = """
    INSERT INTO taxi_trips (
        tpep_pickup_datetime, tpep_dropoff_datetime,
        passenger_count, trip_distance,
        pickup_longitude, pickup_latitude,
        dropoff_longitude, dropoff_latitude,
        fare_amount, tip_amount, total_amount, payment_type
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    try:
        with conn.cursor() as cur:
            for record in generate_trip_data(num_records):
                cur.execute(insert_sql, record)
            conn.commit()
            print(f"Successfully inserted {num_records} records")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        raise

def generate_continuous_data(interval_seconds=5, batch_size=10):
    """Continuously generate and insert data at specified intervals"""
    import time
    conn = create_connection()
    try:
        # Ensure table exists before starting inserts
        create_table(conn)
        insert_data_past(conn,num_records=1000)
        while True:
            insert_data(conn, batch_size)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        conn.close()
        print("Data generation stopped by user.")
    except Exception as e:
        print(f"An error occurred during continuous data generation: {e}")
        conn.rollback()
        raise

def main():
    """Main function to create table and insert data"""
    try:
        # Connect to database
        conn = create_connection()
        
        # Create table
        create_table(conn)
        
        # Insert 1000 records
        insert_data(conn, 1000)
        
        # Close connection
        conn.close()
        print("Data generation completed successfully!")
        
    except Exception as e:
        print(f"An error occurred: {e}", flush=True)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    generate_continuous_data()