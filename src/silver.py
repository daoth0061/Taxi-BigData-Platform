from helpers.utils import get_watermark, get_spark, update_watermark
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp, unix_timestamp, avg, coalesce
from pyspark.sql.functions import udf, StringType
import pyspark.sql.functions as F

spark = get_spark()

@udf(StringType())
def payment_map(payment_type : str) -> StringType: 
    return  {
            '0': "Flex Fare trip",
            '1': "Credit card",
            '2': "Cash",
            '3': "No charge",
            '4': "Dispute",
            '5': "Unknown",
            '6': "Voided trip"
        }.get(payment_type, "N/A")



spark.sql(
    """
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.watermarks (
        table_name STRING,
        last_watermark_ts TIMESTAMP,
        updated_at TIMESTAMP
    ) USING iceberg
    """
)

if not spark.catalog.tableExists("lakehouse.bronze.taxi_trips"):
    print("Bronze table does not exist. Exiting...")
    spark.stop()
    exit(0)


last_updated = get_watermark(spark, "bronze", "taxi_trips")



if last_updated: 
    df = spark.sql(f"""
                   SELECT * from lakehouse.bronze.taxi_trips
                   WHERE tpep_pickup_datetime > '{last_updated}'
                   """)
else :
    df = spark.sql("SELECT * from lakehouse.bronze.taxi_trips")

if not df.rdd.isEmpty():

    df = df.withColumn("tpep_pickup_datetime", to_timestamp(from_unixtime(col("tpep_pickup_datetime") / 1000.0))) \
            .withColumn("tpep_dropoff_datetime", to_timestamp(from_unixtime(col("tpep_dropoff_datetime") / 1000.0))) \
            .withColumn('trip_distance', F.col('trip_distance').cast('float')) \
            .withColumn('fare_amount', F.col('fare_amount').cast('float')) \
            .withColumn('tip_amount', F.col('tip_amount').cast('float')) \
            .withColumn('total_amount', F.col('total_amount').cast('float')) \
            .withColumn('pickup_longitude', F.col('pickup_longitude').cast('float')) \
            .withColumn('pickup_latitude', F.col('pickup_latitude').cast('float')) \
            .withColumn('dropoff_longitude', F.col('dropoff_longitude').cast('float')) \
            .withColumn('dropoff_latitude', F.col('dropoff_latitude').cast('float')) \
            .withColumn('passenger_count', F.col('passenger_count').cast('int'))

    # payment map +  trip duration
    df = df.withColumn("payment_type", payment_map(df.payment_type)) \
        .withColumn('trip_duration_minutes', (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime'))/60) \
        .withColumn('trip_duration_minutes', F.round(F.col('trip_duration_minutes'), 2))
        # .withColumn("zone", )

    # dropna
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"])


    # fillna
    df = df.fillna(float(0), subset=['fare_amount', 'tip_amount', 'total_amount'])


    # distinct
    df = df.distinct()


    update_watermark(spark, "taxi_trips", "bronze", df.agg(F.max("tpep_pickup_datetime")).first()[0])

    df.printSchema()
    df.createOrReplaceTempView("tmp_taxi_trips")

    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.silver.taxi_trips
    USING iceberg
    AS SELECT * FROM tmp_taxi_trips
    """)


    df.writeTo("lakehouse.silver.taxi_trips").append()



spark.stop()