WITH raw_data AS (
    -- Green data
    SELECT
        vendorid,
        pulocationid,
        dolocationid,
        lpep_pickup_datetime AS pickup_datetime,
        total_amount,
        fare_amount,
        trip_distance,
        payment_type,
        'green' AS service_type
    FROM {{ source('raw_nyc_tripdata', 'ext_green') }}
    WHERE vendorid IS NOT NULL

    UNION ALL

    -- Yellow data
    SELECT
        vendorid,
        pulocationid,
        dolocationid,
        tpep_pickup_datetime AS pickup_datetime,
        total_amount,
        fare_amount,
        trip_distance,
        payment_type,
        'yellow' AS service_type
    FROM {{ source('raw_nyc_tripdata', 'ext_yellow') }}
    WHERE vendorid IS NOT NULL
)

SELECT
    pulocationid,
    dolocationid,
    service_type,
    total_amount,
    fare_amount,
    trip_distance,
    FORMAT_DATE('%Y/Q%Q', pickup_datetime) AS year_quarter,
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'EMPTY'
    END AS payment_type_description
FROM raw_data
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY service_type, vendorid, pickup_datetime) = 1
