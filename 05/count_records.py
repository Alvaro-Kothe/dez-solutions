from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CountRecords").getOrCreate()

df = spark.read.parquet("./repartitioned/")

df_oct_15 = df.filter((col("tpep_pickup_datetime").cast("date") == "2024-10-15"))

total_records = df_oct_15.count()

print(f"Total records: {total_records}")

spark.stop()
