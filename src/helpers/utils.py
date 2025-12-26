from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from datetime import datetime
import os

def update_watermark(spark, table_name, layer, new_watermark_ts):


    print(new_watermark_ts)
    
    schema = StructType([
        StructField("table_name", StringType(), False), 
        StructField("last_watermark_ts", LongType(), False),  #  last_watermark 
        StructField("updated_at", TimestampType(), False) # update_time
    ])

    watermark_df = spark.createDataFrame(
        [(table_name, new_watermark_ts, datetime.now())],
        schema=schema
    )
    spark.sql(f"DELETE FROM lakehouse.{layer}.watermarks WHERE table_name = '{table_name}'")
    watermark_df.writeTo(f"lakehouse.{layer}.watermarks").append()



def get_watermark(spark, layer, table_name):
    result = spark.sql(
        f"SELECT last_watermark_ts FROM lakehouse.{layer}.watermarks WHERE table_name = '{table_name}' ORDER BY updated_at DESC LIMIT 1"
    ).collect()
    return result[0][0] if result else None


def get_spark():
    existing_spark = SparkSession.getActiveSession()
    if existing_spark is not None:
        return existing_spark

    s3a_endpoint = os.getenv("S3A_ENDPOINT", "http://localhost:9000")
    s3a_access_key = os.getenv("S3A_ACCESS_KEY", "admin")
    s3a_secret_key = os.getenv("S3A_SECRET_KEY", "12345678")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://lakehouse/warehouse")
    
    spark = SparkSession.builder.appName("TaxiStreamToIceberg") \
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.767",
            ])
        ) \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", iceberg_warehouse) \
        .config("spark.hadoop.fs.s3a.endpoint", s3a_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", s3a_access_key) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.hadoop.fs.s3a.secret.key", s3a_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    return spark
