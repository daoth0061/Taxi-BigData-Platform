from helpers.utils import get_watermark, get_spark, update_watermark
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, unix_timestamp, avg, coalesce
from pyspark.sql.functions import udf, StringType
import pyspark.sql.functions as F

spark = get_spark()


spark.sql(
    """
    CREATE TABLE IF NOT EXISTS lakehouse.silver.watermarks (
        table_name STRING,
        last_watermark_ts TIMESTAMP,
        updated_at TIMESTAMP
    ) USING iceberg
    """
)

last_updated = get_watermark(spark, "silver", "taxi_trips")



if last_updated: 
    df = spark.sql(f"""
                   SELECT * from lakehouse.silver.taxi_trips
                   WHERE tpep_pickup_datetime > '{last_updated}'
                   """)
else :
    df = spark.sql("SELECT * from lakehouse.silver.taxi_trips")


if not df.rdd.isEmpty():

    ### Dim dates

    unique_dates = df.select(F.col("tpep_pickup_datetime").alias("date_key") \
                             .union(df.select(F.col("tpep_dropoff_datetime").alias("date_key")))) \
                             .distinct().dropna()

    dim_date_df = unique_dates.withColumn(
        "datetime_sk", F.date_format(col("date_key"), "yyyyMMdd").cast("int")
    ).withColumn(
        "year", F.year("date_key")
    ).withColumn(
        "month", F.month("date_key")
    ).withColumn(
        "day", F.dayofmonth("date_key")
    ).withColumn(
        "day_of_week", F.dayofweek("date_key")
    ).withColumn(
        "day_name", F.date_format("date_key", "EEEE")
    ).select(
        "datetime_sk", "date_key", "year", "month", "day", "day_of_week", "day_name"
    )

    dim_date_df.createOrReplaceTempView("dim_date_temp")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date (
            datetime_sk INT,
            date_key DATE,
            year INT,
            month INT,
            day INT,
            day_of_week INT,
            day_name STRING
        ) USING iceberg
    """)
    
    spark.sql("""
        MERGE INTO lakehouse.gold.dim_date AS target
        USING dim_date_temp AS source
        ON target.datetime_sk = source.datetime_sk
        WHEN NOT MATCHED THEN
        INSERT *
    """)