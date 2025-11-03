from helpers.utils import get_watermark, get_spark, update_watermark
from pyspark.sql.functions import udf, StringType
import pyspark.sql.functions as F
spark = get_spark()

@udf(StringType())
def payment_map() -> StringType: 
    return udf(
        lambda payment_type: {
            0 : "Flex Fare trip",
            1: "Credit card",
            2: "Cash",
            3: "No charge",
            4: "Dispute",
            5: "Unknown",
            6: "Voided trip"
        }.get(payment_type, "N/A"), StringType()
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


spark.sql(
    """
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.watermarks (
        table_name STRING,
        last_watermark_ts TIMESTAMP,
        updated_at TIMESTAMP
    ) USING iceberg
    """
)

last_updated = get_watermark(spark, "bronze", "taxi_trips")



if last_updated: 
    df = spark.sql(f"""
                   SELECT * from lakehouse.bronze.taxi_trips
                   WHERE tpep_pickup_datetime > '{last_updated}'
                   """)
else :
    df = spark.sql("SELECT * from lakehouse.bronze.taxi_trips")

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


# update_watermark(spark, "taxi_trips", "bronze", df.agg(F.max("tpep_pickup_datetime")).first()[0])

df.createOrReplaceTempView("tmp_taxi_trips")

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.taxi_trips
USING iceberg
AS SELECT * FROM tmp_taxi_trips
""")


df.writeTo("lakehouse.silver.taxi_trips").append()

spark.stop()