WITH dim_zones AS (
    SELECT
        locationid,
        zone
    FROM {{ ref('taxi_zone_lookup') }}
    WHERE borough != 'Unknown'
)

SELECT
    t.year,
    t.month,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pulocationid,
    t.dolocationid,
    pu.zone AS pickup_zone,
    do.zone AS dropoff_zone
FROM {{ ref("stg_fhv") }} AS t
INNER JOIN dim_zones AS pu
    ON t.pulocationid = pu.locationid
INNER JOIN dim_zones AS do
    ON t.dolocationid = do.locationid
