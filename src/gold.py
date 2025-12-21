from helpers.utils import get_watermark, get_spark, update_watermark
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, unix_timestamp, avg, coalesce
from pyspark.sql.functions import udf, StringType
import pyspark.sql.functions as F

dim_zone_path = "s3a://lakehouse/taxi_zone.csv"

spark = get_spark()


spark.sql(
    """
    CREATE TABLE IF NOT EXISTS lakehouse.silver.watermarks (
        table_name STRING,
        last_watermark_ts LONG,
        updated_at TIMESTAMP
    ) USING iceberg
    """
)

if not spark.catalog.tableExists("lakehouse.silver.taxi_trips"):
    print("Silver table does not exist. Exiting...")
    spark.stop()
    exit(0)


last_updated = get_watermark(spark, "silver", "taxi_trips")



if last_updated: 
    df = spark.sql(f"""
                   SELECT * from lakehouse.bronze.taxi_trips
                   WHERE CAST(tpep_pickup_datetime AS LONG)> '{last_updated}'
                   """)   
else :
    df = spark.sql("SELECT * from lakehouse.silver.taxi_trips")


if not df.rdd.isEmpty():

    new_ts = df.agg(F.max("tpep_pickup_datetime").cast("long")).first()[0]

    ### Dim datetime Table

    pickup_df = df.select(F.col("tpep_pickup_datetime").alias("date_key"))

    dropoff_df = df.select(F.col("tpep_dropoff_datetime").alias("date_key"))

    unique_dates = pickup_df.union(dropoff_df).distinct().dropna()

    # surrogate key would be in the format YYYYMMDD since it would always be 

    unique_datetimes = df.select(
        F.col("tpep_pickup_datetime").alias("dt_key")
    ).union(
        df.select(F.col("tpep_dropoff_datetime").alias("dt_key"))
    ).distinct().dropna()

    dim_datetime_df = unique_datetimes.withColumn(
        "datetime_sk", F.date_format(col("dt_key"), "yyyyMMddHH").cast("long")
    ).withColumn(
        "date_key", F.to_date("dt_key")
    ).withColumn(
        "year", F.year("dt_key")
    ).withColumn(
        "month", F.month("dt_key")
    ).withColumn(
        "day", F.dayofmonth("dt_key")
    ).withColumn(
        "hour", F.hour("dt_key")
    ).withColumn(
        "day_of_week", F.dayofweek("dt_key")  # 1 = Sunday ... 7 = Saturday
    ).withColumn(
        "day_name", F.date_format("dt_key", "EEEE")
    ).withColumn(
        # Rush hour definition: 7–9 AM and 4–6 PM
        "is_rush_hour",
        F.expr("hour IN (7, 8, 9, 16, 17, 18)").cast("boolean")
    ).withColumn(
        # Weekend based on Spark's day_of_week: 1=Sunday, 7=Saturday
        "is_weekend",
        F.expr("day_of_week IN (1, 7)").cast("boolean")
    ).select(
        "datetime_sk",
        "dt_key",
        "date_key",
        "year",
        "month",
        "day",
        "hour",
        "day_of_week",
        "day_name",
        "is_rush_hour",
        "is_weekend"
    )


    dim_datetime_df.createOrReplaceTempView("dim_datetime_temp")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_datetime (
            datetime_sk BIGINT,
            dt_key TIMESTAMP,
            date_key DATE,
            year INT,
            month INT,
            day INT,
            hour INT,
            day_of_week INT,
            day_name STRING,
            is_rush_hour BOOLEAN,
            is_weekend BOOLEAN
        ) USING iceberg
    """)
    
    spark.sql("""
        MERGE INTO lakehouse.gold.dim_datetime AS target
        USING dim_datetime_temp AS source
        ON target.datetime_sk = source.datetime_sk
        WHEN NOT MATCHED THEN
        INSERT *
    """)


    ### Dim zone df 

    dim_zone_df = spark.read.csv(
        dim_zone_path,
        header=True,
        inferSchema=True) 

    if not spark.catalog.tableExists("lakehouse.gold.dim_zone"):
        dim_zone_df.writeTo("lakehouse.gold.dim_zone").using("iceberg").create()

    fact_trip_df = (
        df
        .withColumn(
            "pickup_datetime_sk",
            F.date_format(col("tpep_pickup_datetime"), "yyyyMMddHH").cast("long")
        )
        .withColumn(
            "dropoff_datetime_sk",
            F.date_format(col("tpep_dropoff_datetime"), "yyyyMMddHH").cast("long")
        )
        .join(
            dim_zone_df.alias("p"),
            (df.pickup_latitude.between(F.col("p.lat_min"), F.col("p.lat_max"))) &
            (df.pickup_longitude.between(F.col("p.lon_min"), F.col("p.lon_max"))),
            "left"
        )
        .join(
            dim_zone_df.alias("d"),
            (df.dropoff_latitude.between(F.col("d.lat_min"), F.col("d.lat_max"))) &
            (df.dropoff_longitude.between(F.col("d.lon_min"), F.col("d.lon_max"))),
            "left"
        )
        .select(
            "pickup_datetime_sk",
            "dropoff_datetime_sk",
            F.coalesce(F.col("p.zone_id"), F.lit(-1)).alias("pickup_zone_id"),
            F.coalesce(F.col("d.zone_id"), F.lit(-1)).alias("dropoff_zone_id"),
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "payment_type",
            "trip_duration_minutes"
        )
    )

    fact_trip_df.createOrReplaceTempView("fact_trip_temp")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_trip (
            pickup_datetime_sk BIGINT,
            dropoff_datetime_sk BIGINT,
            pickup_zone_id INT,
            dropoff_zone_id INT,
            passenger_count INT,
            trip_distance FLOAT,
            fare_amount FLOAT,
            tip_amount FLOAT,
            total_amount FLOAT,
            payment_type STRING,
            trip_duration_minutes FLOAT
        ) USING iceberg
    """)
    
    spark.sql("""
        MERGE INTO lakehouse.gold.fact_trip AS target
        USING fact_trip_temp AS source
        ON target.pickup_datetime_sk = source.pickup_datetime_sk
        AND target.dropoff_datetime_sk = source.dropoff_datetime_sk
        AND target.pickup_zone_id = source.pickup_zone_id
        AND target.dropoff_zone_id = source.dropoff_zone_id
        WHEN NOT MATCHED THEN
        INSERT *
    """)

    update_watermark(spark, "taxi_trips", "silver", new_ts)

