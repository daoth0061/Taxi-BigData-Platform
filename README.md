- From the repository root:

```powershell
# Start Postgres, Kafka, Debezium, Kafka UI
docker compose up -d

# Check health and status
docker compose ps
```

What happens automatically:
- Postgres starts with logical replication enabled.
- Debezium Connect becomes healthy.
- A small one-shot service (debezium-init) registers the connector using `configs/debezium-connector.json`.

Verify (optional):
```powershell
# Should list the connector name: taxi-postgres-connector
curl http://localhost:8083/connectors

# Open Kafka UI in a browser
start http://localhost:8080
```

## 3) Install Python dependencies (host)
```powershell
# Optional but recommended: create and activate a venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install deps
pip install -r requirements.txt
```

## 4) Generate taxi trip data (host)
The generator writes directly to Postgres (exposed on localhost:5432) and runs continuously until you stop it.

```powershell
python src/data_generation/insert_taxi_data.py
```

- Stop with Ctrl+C.
- Data inserts will flow to Kafka via Debezium; inspect topics in Kafka UI.

## 5) Initialize Spark Streaming from Kafka to Iceberg
```powershell
python src/streaming/kafka_to_iceberg.py
```
## 6) Connect Spark SQL in Superset 

- SparkSQL connection URL 
```
hive://hive@spark-thrift:10000/default
```
## 7) Upload src/taxi_zone.csv to Minio at localhost:9001 

## 8) Tear down
```powershell
# Stop and remove containers and data volumes
docker compose down -v
```
