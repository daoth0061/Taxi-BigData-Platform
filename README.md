# NYC Taxi Data Streaming Pipeline - Setup Guide

## 1. Start Infrastructure Services

From the repository root:
```powershell
docker compose up -d
```

**What starts automatically:**
- Postgres (with logical replication enabled)
- Kafka and Debezium Connect
- Kafka UI, Spark, MinIO, Flink, Cassandra, Superset
- Debezium connector auto-registers using `configs/debezium-connector.json`

**Verify (optional):**
```powershell
# Check services are healthy
docker compose ps

# Verify connector is registered (should return: ["taxi-postgres-connector"])
curl http://localhost:8083/connectors

# Open Kafka UI
start http://localhost:8080
```

---

## 2. Install Python Dependencies
```powershell
# Create and activate virtual environment (recommended)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

---

## 3. Generate Taxi Trip Data

Writes directly to Postgres (exposed on localhost:5432). Runs continuously until stopped.
```powershell
python src/data_generation/insert_taxi_data.py
```

- Stop with `Ctrl+C`
- Data flows to Kafka via Debezium automatically
- Inspect topics in Kafka UI at http://localhost:8080

---

## 4. Start Spark Streaming (Kafka → Iceberg)
```powershell
python src/streaming/kafka_to_iceberg.py
```

---

## 5. Upload Taxi Zone Data to MinIO

- Open MinIO console at http://localhost:9001
- Upload `src/taxi_zone.csv`

---

## 6. Connect Superset to Spark SQL

Use this connection URL in Superset:
```
hive://hive@spark-thrift:10000/default
```

---

## 7. Set Up Flink and Cassandra

### Create Cassandra Tables
```bash
docker exec -it cassandra cqlsh
```
```sql
CREATE KEYSPACE IF NOT EXISTS realtime 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS realtime.revenue_minute (
    window_start timestamp PRIMARY KEY,
    window_end timestamp,
    total_revenue double,
    trip_count bigint
);

CREATE TABLE IF NOT EXISTS realtime.active_trips (
    window_start timestamp PRIMARY KEY,
    window_end timestamp,
    active_trips bigint
);
```

Type `exit` to close CQL shell.

### Build and Deploy Flink Application
```bash
cd src/flink

./gradlew clean shadowJar

docker cp app/build/libs/app-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/

docker exec -it flink-jobmanager flink run \
  --class org.streaming.Application \
  /opt/flink/app-1.0-SNAPSHOT.jar
```

---

## 8. Tear Down

Stop and remove all containers and data volumes:
```powershell
docker compose down -v
```