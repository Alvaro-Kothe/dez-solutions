#!/usr/bin/env sh

# Read csv file from stdin and fill the green_trip table, avoiding duplicates
host=${DB_HOST:-localhost}
PSQL="psql -h $host -U postgres -d taxi"

create_staging_table="create temp table green_trip_staging as select * from green_trip limit 0"
import_csv_data_into_staging="copy green_trip_staging from stdin with (format csv, header true)"
merge_staging_data="
insert into green_trip 
  select * from green_trip_staging as S
  where not exists(
    select 1 
    from green_trip as H 
    where H.lpep_pickup_datetime = S.lpep_pickup_datetime and 
    H.lpep_dropoff_datetime = S.lpep_dropoff_datetime and 
    H.pulocationid = S.pulocationid and 
    H.dolocationid = S.dolocationid and 
    H.fare_amount = S.fare_amount and 
    H.trip_distance = S.trip_distance)
"

$PSQL -c "BEGIN" \
  -c "$create_staging_table" \
  -c "$import_csv_data_into_staging" \
  -c "$merge_staging_data" \
  -c "COMMIT" <&0
