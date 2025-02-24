from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LeastFrequentPickupZone").getOrCreate()

zone_df = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)
zone_df.createOrReplaceTempView("zone_lookup")

df = spark.read.parquet("./repartitioned/")
df.createOrReplaceTempView("yellow_trip")

result = spark.sql("""
    SELECT z.Zone, COUNT(t.PULocationID) AS pickup_count
    FROM yellow_trip t
    JOIN zone_lookup z
    ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY pickup_count ASC
    LIMIT 1
""")

result.show(truncate=False)
