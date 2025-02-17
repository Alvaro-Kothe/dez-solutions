SELECT
    pulocationid,
    dolocationid,
    pickup_datetime,
    dropoff_datetime,
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(MONTH FROM pickup_datetime) AS month
FROM {{ source('raw_nyc_tripdata', 'ext_fhv') }}
WHERE dispatching_base_num IS NOT NULL

{% if var('is_test_run', default=true) %}

LIMIT 100

{% endif %}
