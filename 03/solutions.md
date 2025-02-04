# Solutions

1. 20,332,093

   In the storage info -> number of rows for the native table it says that it contains 20,332,093 rows.

2. external: 0B; materialized: 155.12 MB

   ```
   SELECT COUNT(DISTINCT(PULocationID))
   FROM `taxi_data.yellow_trip_native`; -- This query will process 155.12 MB when run.

   SELECT COUNT(DISTINCT(PULocationID))
   FROM `taxi_data.yellow_trip_external`; -- This query will process 0 B when run.
   ```

3. BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

   The cost is determined by the amount of data scanned.
   As it stores data in a columnar approach, selecting more columns increases the amount of data scanned.

4. 8333

   ```sql
   SELECT COUNT(*) FROM `environment-dez.taxi_data.yellow_trip_external` WHERE fare_amount = 0;
   ```

   ```
   Row 	f0_
   1 	8333
   ```

5. Partition by tpep_dropoff_datetime and Cluster on VendorID

   > A partitioned table is divided into segments, called partitions, that make it easier to manage and query your data. By dividing a large table into smaller partitions, you can improve query performance and control costs by reducing the number of bytes read by a query. You partition tables by specifying a partition column which is used to segment the table.

   > Clustered tables in BigQuery are tables that have a user-defined column sort order using clustered columns. Clustered tables can improve query performance and reduce query costs.

   ```sql
   CREATE TABLE
   taxi_data.yellow_trip_part_clust
   PARTITION BY
   DATE_TRUNC(tpep_dropoff_datetime, DAY)
   CLUSTER BY
   VendorID
   AS (
   SELECT *
   FROM
       `taxi_data.yellow_trip_external`
   )
   ```

   > [!NOTE]
   > There is a typo tpep_dropoff_timedate -> tpep_dropoff_datetime

6. 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

   ```sql
   CREATE TABLE
   taxi_data.yellow_trip_part_clust
   PARTITION BY
     DATE_TRUNC(tpep_dropoff_datetime, DAY)
   CLUSTER BY
     VendorID
   AS (
     SELECT *
     FROM
       `taxi_data.yellow_trip_external`
   ); -- This query will process 26.84 MB when run.

   SELECT DISTINCT(VendorID)
   FROM `taxi_data.yellow_trip_native`
   WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'; -- This query will process 310.24 MB when run.
   ```

7. GCP Bucket

   ```
   Source URI(s): gs://taxi-data-12abf3/yellow_tripdata_2024-*.parquet
   ```

8. false

   > That depends on your workload. If all your queries include the partitioning column in the filter conditions, you may be able to see some performance gain. But if you have plenty of queries that do not, performance may degrade for those question.
