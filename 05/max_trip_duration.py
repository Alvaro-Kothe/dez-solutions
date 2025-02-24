from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, max

spark = SparkSession.builder.appName("MaxTripDuration").getOrCreate()

df = spark.read.parquet("./repartitioned/")

df_with_duration = df.withColumn(
    "trip_duration_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))
    / 3600,
)

max_trip_duration = df_with_duration.agg(max("trip_duration_hours")).collect()[0][0]

print(f"Maximum trip duration: {max_trip_duration}")

spark.stop()
