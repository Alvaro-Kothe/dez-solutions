WITH trip_duration_p90 AS (
    SELECT
        year,
        month,
        pickup_zone,
        dropoff_zone,
        PERCENTILE_CONT(
            TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 0.90
        )
            OVER (PARTITION BY year, month, pulocationid, dolocationid)
            AS p90
    FROM {{ ref("dim_fhv_trips") }}
)

SELECT
    year,
    month,
    pickup_zone,
    dropoff_zone,
    ANY_VALUE(p90) AS p90_trip_duration
FROM trip_duration_p90
GROUP BY year, month, pickup_zone, dropoff_zone
