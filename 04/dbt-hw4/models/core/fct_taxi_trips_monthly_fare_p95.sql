WITH valid_entries AS (
    SELECT
        service_type,
        year,
        month,
        fare_amount
    FROM {{ ref("dim_taxi_trips") }}
    WHERE
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit Card')
),

percentiles_full AS (
    SELECT
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97)
            OVER (PARTITION BY service_type, year, month)
            AS p97,
        PERCENTILE_CONT(fare_amount, 0.95)
            OVER (PARTITION BY service_type, year, month)
            AS p95,
        PERCENTILE_CONT(fare_amount, 0.90)
            OVER (PARTITION BY service_type, year, month)
            AS p90
    FROM valid_entries
)

SELECT
    service_type,
    year,
    month,
    ANY_VALUE(p97) AS p97,
    ANY_VALUE(p95) AS p95,
    ANY_VALUE(p90) AS p90
FROM percentiles_full
GROUP BY service_type, year, month
