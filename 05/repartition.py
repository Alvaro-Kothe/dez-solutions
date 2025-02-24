from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RepartitionParquet").getOrCreate()

df = spark.read.parquet("./yellow_tripdata_2024-10.parquet")
df_repartitioned = df.repartition(4)

df_repartitioned.write.mode("overwrite").parquet("repartitioned")

spark.stop()
