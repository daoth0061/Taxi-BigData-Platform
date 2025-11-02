from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, unix_timestamp, avg, coalesce, max as max_, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType, LongType
from pyspark.sql.streaming import StreamingQueryListener
from datetime import datetime
import json


def get_spark():
    return (
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


class LogThroughput(StreamingQueryListener):
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
                flush=True,
            )
        except Exception:
            print(getattr(event.progress, 'prettyJson', getattr(event.progress, 'json', str(event.progress))))

    def onQueryTerminated(self, event):
        pass


def ensure_table(spark):
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
    spark.sql(
        """
        ALTER TABLE lakehouse.bronze.taxi_trips SET TBLPROPERTIES (
            'format-version'='2',
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read'
        )
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.watermarks (
            table_name STRING,
            last_watermark_ts TIMESTAMP,
            updated_at TIMESTAMP
        ) USING iceberg
        """
    )


def get_watermark(spark, table_name):
    result = spark.sql(
        f"SELECT last_watermark_ts FROM lakehouse.bronze.watermarks WHERE table_name = '{table_name}' ORDER BY updated_at DESC LIMIT 1"
    ).collect()
    return result[0][0] if result else None


def update_watermark(spark, table_name, new_watermark_ts):
    watermark_df = spark.createDataFrame(
        [(table_name, new_watermark_ts, datetime.now())],
        ["table_name", "last_watermark_ts", "updated_at"]
    )
    spark.sql(f"DELETE FROM lakehouse.bronze.watermarks WHERE table_name = '{table_name}'")
    watermark_df.writeTo("lakehouse.bronze.watermarks").append()


def build_schema():
    return StructType([
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
        StructField("op", StringType()),
        StructField("source.ts_ms", LongType()),
    ])


def build_stream(spark, schema):
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "taxi.public.taxi_trips")
        .option("startingOffsets", "earliest")
        .load()
    )
    df_json = (
        df_kafka.filter(col("value").isNotNull()).selectExpr("CAST(value AS STRING) AS value", "timestamp AS kafka_ts")
    )
    df_parsed = df_json.select(from_json(col("value"), schema).alias("data"), col("kafka_ts")).select("data.*", "kafka_ts")
    df_final = (
        df_parsed
        .filter((col("op").isNull()) | (col("op") != "d"))
        .withColumn("tpep_pickup_datetime", to_timestamp(from_unixtime(col("tpep_pickup_datetime") / 1000.0)))
        .withColumn("tpep_dropoff_datetime", to_timestamp(from_unixtime(col("tpep_dropoff_datetime") / 1000.0)))
        .withColumn("source_ts_ms", col("`source.ts_ms`"))
        .withColumn("kafka_ts_ms", (unix_timestamp(col("kafka_ts")).cast("long") * 1000))
        .drop("op")
    )
    return df_final


def write_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        print(f"[write_batch] batch={batch_id} is empty, skipping", flush=True)
        return
    
    print(f"[write_batch] batch={batch_id} processing {batch_df.count()} rows", flush=True)
    
    lat_df = batch_df.select(
        (unix_timestamp().cast("long") * 1000 - coalesce(col("source_ts_ms").cast("long"), col("kafka_ts_ms").cast("long"))).alias("latency_ms")
    )
    avg_lat = lat_df.agg(avg("latency_ms").alias("avg_ms")).collect()[0][0]
    print(f"[latency] batch={batch_id} avg_ms={int(avg_lat) if avg_lat is not None else 'NA'}", flush=True)
    
    out_df = batch_df.drop("source_ts_ms", "kafka_ts_ms", "kafka_ts").drop(col("`source.ts_ms`"), "source")
    out_df.writeTo("lakehouse.bronze.taxi_trips").append()
    
    max_source_ts = batch_df.select(
        coalesce(max_(col("source_ts_ms")), max_(col("kafka_ts_ms"))).alias("max_ts")
    ).collect()[0][0]
    
    if max_source_ts:
        max_ts_timestamp = datetime.fromtimestamp(max_source_ts / 1000.0)
        spark = batch_df.sparkSession
        update_watermark(spark, "taxi_trips", max_ts_timestamp)
        print(f"[watermark] batch={batch_id} updated to {max_ts_timestamp}", flush=True)
    else:
        print(f"[watermark] batch={batch_id} skipped - max_source_ts is None", flush=True)


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(LogThroughput())
    ensure_table(spark)
    schema = build_schema()
    df_final = build_stream(spark, schema)
    query = (
        df_final.writeStream.outputMode("append").option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze_taxi_trips").trigger(processingTime="10 seconds").foreachBatch(write_batch).start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()