import sys
import os

# Thêm path để import helpers khi chạy từ Spark submit
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from helpers.utils import get_watermark, get_spark, update_watermark
from pyspark.sql.functions import udf, StringType
import pyspark.sql.functions as F

spark = get_spark()

df = spark.sql("""
               SELECT * from lakehouse.silver.taxi_trips
               """)

df = df.withColumn()

spark.stop()