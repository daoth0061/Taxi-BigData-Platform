from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType, LongType
from pyspark.sql.streaming import StreamingQueryListener
import json

## Init Spark Session
spark = (
    SparkSession.builder
    .appName("TaxiStreamToIceberg")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.767",
        ])
    )
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.hadoop.fs.s3a.secret.key", "12345678")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Lightweight per-batch throughput logging
class _LogThroughput(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        try:
            p = json.loads(event.progress.json)
            print(
                f"[batch {p.get('batchId')}] "
                f"inputRows={p.get('numInputRows')} "
                f"inRate={p.get('inputRowsPerSecond')} r/s "
                f"procRate={p.get('processedRowsPerSecond')} r/s "
                f"durationMs={p.get('durationMs')}",
                flush=True
            )
        except Exception:
            # Fallback to raw JSON if structure changes
            print(getattr(event.progress, 'prettyJson', getattr(event.progress, 'json', str(event.progress))))

    def onQueryTerminated(self, event):
        pass

spark.streams.addListener(_LogThroughput())

# Ensure target Iceberg namespace/table exist (no-op if already present)
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.taxi_trips (
            id INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance FLOAT,
            pickup_longitude FLOAT,
            pickup_latitude FLOAT,
            dropoff_longitude FLOAT,
            dropoff_latitude FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT
        ) USING iceberg
        """
)

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "taxi.public.taxi_trips")
    .option("startingOffsets", "earliest")
    .load()
)

df_json = df_kafka.filter(col("value").isNotNull()).selectExpr("CAST(value AS STRING)")

schema = StructType([
    StructField("id", IntegerType()),
    StructField("tpep_pickup_datetime", LongType()),  
    StructField("tpep_dropoff_datetime", LongType()),  
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", FloatType()),
    StructField("pickup_longitude", FloatType()),
    StructField("pickup_latitude", FloatType()),
    StructField("dropoff_longitude", FloatType()),
    StructField("dropoff_latitude", FloatType()),
    StructField("fare_amount", FloatType()),
    StructField("tip_amount", FloatType()),
    StructField("total_amount", FloatType()),
    StructField("op", StringType())
])

df_parsed = df_json.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert epoch ms -> timestamp, drop deletes and helper columns
df_final = (
    df_parsed
    .filter((col("op").isNull()) | (col("op") != "d"))
    .withColumn("tpep_pickup_datetime", to_timestamp(from_unixtime(col("tpep_pickup_datetime") / 1000.0)))
    .withColumn("tpep_dropoff_datetime", to_timestamp(from_unixtime(col("tpep_dropoff_datetime") / 1000.0)))
    .drop("op")
)

# Stream to Iceberg, Bronze level
query = (
    df_final.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze_taxi_trips")
    .trigger(processingTime="10 seconds")
    .toTable("lakehouse.bronze.taxi_trips")
)

query.awaitTermination()