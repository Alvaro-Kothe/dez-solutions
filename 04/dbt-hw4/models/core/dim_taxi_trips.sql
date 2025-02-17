WITH dim_zones AS (
    SELECT
        locationid,
        zone
    FROM {{ ref('taxi_zone_lookup') }}
    WHERE borough != 'Unknown'
)

SELECT t.*
FROM {{ ref("stg_taxi_trips") }} AS t
INNER JOIN dim_zones AS pu
    ON t.pulocationid = pu.locationid
INNER JOIN dim_zones AS do
    ON t.dolocationid = do.locationid
