# Solutions

1. `3.5.4`

    - Install pyspark with `pip install pyspark`

    - Get version

    ```pythonconsole
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> spark.version
    '3.5.4'
    ```

2. `25 MB`

    ```console
    $ python repartition.py
    $ ls -lh repartitioned
    total 90M
    -rw-r--r--. 1 user user 23M fev 23 20:53 part-00000-6d5cf882-529c-4cba-a9d1-55aea69677f3-c000.snappy.parquet
    -rw-r--r--. 1 user user 23M fev 23 20:53 part-00001-6d5cf882-529c-4cba-a9d1-55aea69677f3-c000.snappy.parquet
    -rw-r--r--. 1 user user 23M fev 23 20:53 part-00002-6d5cf882-529c-4cba-a9d1-55aea69677f3-c000.snappy.parquet
    -rw-r--r--. 1 user user 23M fev 23 20:53 part-00003-6d5cf882-529c-4cba-a9d1-55aea69677f3-c000.snappy.parquet
    -rw-r--r--. 1 user user   0 fev 23 20:53 _SUCCESS
    ```

3. 125,567

    ```console
    $ python count_records.py
    Total records: 128893
    ```

4. 162

    ```console
    $ python max_trip_duration.py
    Maximum trip duration: 162.61777777777777
    ```

5. 4040

6. Governor's Island/Ellis Island/Liberty Island

    ```console
    $ python least_frequent_pu_zone.py
    +---------------------------------------------+------------+
    |Zone                                         |pickup_count|
    +---------------------------------------------+------------+
    |Governor's Island/Ellis Island/Liberty Island|1           |
    +---------------------------------------------+------------+
    ```
