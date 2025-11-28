#!/usr/bin/env python3
from airflow import settings
from airflow.models import Connection
import sys

# Delete old connection if exists
session = settings.Session()
conn = session.query(Connection).filter(Connection.conn_id == 'spark_default').first()
if conn:
    session.delete(conn)
    session.commit()
    print("Deleted old spark_default connection")

# Create new connection
new_conn = Connection(
    conn_id='spark_default',
    conn_type='spark',
    host='spark://spark-master',
    port=7077,
    extra='{"deploy-mode": "client"}'
)

session.add(new_conn)
session.commit()
print("Created new spark_default connection")
print(f"Connection URI: {new_conn.get_uri()}")

session.close()
