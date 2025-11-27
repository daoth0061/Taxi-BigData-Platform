#!/bin/bash

# Đợi Airflow sẵn sàng
sleep 10

# Tạo Spark connection
airflow connections delete spark_default 2>/dev/null || true
airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077

echo "Spark connection created successfully!"
