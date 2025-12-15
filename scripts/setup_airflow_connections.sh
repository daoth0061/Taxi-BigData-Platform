#!/bin/bash

# Wait for Airflow to be fully up and running
sleep 10

# Create Spark connection
airflow connections delete spark_default 2>/dev/null || true
airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077

echo "Spark connection created successfully!"
