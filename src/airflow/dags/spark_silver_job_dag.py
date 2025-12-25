from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_silver_job',
    default_args=default_args,
    description='Run Spark Silver job daily',
    schedule='0 2 * * *',
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['spark', 'silver'],
) as dag:

    run_silver = SparkSubmitOperator(
        task_id='run_silver_job',
        application='/opt/airflow/src/silver.py',
        conn_id='spark_default',
        deploy_mode='client',
        spark_binary='spark-submit',
        executor_cores=2,
        executor_memory='1g',
        driver_memory='512m',
        total_executor_cores=2,
        verbose=True,
        name='silver-job',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.767',
        conf={
            'spark.pyspark.python': 'python3',
            'spark.pyspark.driver.python': 'python3',
        },
    )

    run_silver
