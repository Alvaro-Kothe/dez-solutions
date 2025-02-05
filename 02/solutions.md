# Module 2

I tried initially to solve using mostly `kestra`,
but it deleted the flow that I was creating.

## Solutions

1. 128.3

    Method 1: Download the file, unzip and count bytes

    ```
    $ wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz | gunzip | wc -c
    134481400
    ```

    $134481400 / 2^{20} = 128.251 \text{MiB}$

    Method 2: See the logs that I created in airflow:

    ```
    [2025-01-23, 02:54:05 UTC] {import_taxi_data.py:104} INFO - Downloading data for yellow, 2020-12 from https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz
    [2025-01-23, 02:54:06 UTC] {import_taxi_data.py:115} INFO - Downloaded 25.3 MB
    [2025-01-23, 02:54:10 UTC] {import_taxi_data.py:122} INFO - Inserted 128.3 MB
    ```

2. `green_tripdata_2020-04.csv`

    Considering that `file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"`,
    so `inputs.taxi = green`, `inputs.year = 2020` and `inputs.month = 04`

3. 24,648,499

    ```sql
    SELECT count(*) FROM yellow_trip as t
    INNER JOIN inserted_data as i
    ON i.insertedid = t.insertedid
    WHERE i."year" = 2020;
    ```

    ```
      count
    ----------
     24648499
    (1 row)
    ```

4. 1,734,051

    ```sql
    SELECT count(*) FROM green_trip as t
    INNER JOIN inserted_data as i
    ON i.insertedid = t.insertedid
    WHERE i."year" = 2020;
    ```

    ```
      count
    ---------
     1734051
    (1 row)
    ```

5. 1,925,152

    ```sql
    SELECT count(*) FROM yellow_trip as t
    INNER JOIN inserted_data as i
    ON i.insertedid = t.insertedid
    WHERE i."year" = 2021 AND i."month" = 3;
    ```

    ```
      count
    ---------
     1925152
    (1 row)
    ```

6. Add a timezone property set to America/New_York in the Schedule trigger configuration

    [Schedule Trigger][schedule-trigger]

[schedule-trigger]: https://kestra.io/docs/workflow-components/triggers/schedule-trigger
