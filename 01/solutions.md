# Solutions

1. 24.3.1

    First terminal session is to start the image:

    ```console
    $ podman run --interactive --rm --name python3_12_8 python:3.12.8
    ```

    After the container is running, you can use the `exec` command to get the pip version:

    ```console
    $ podman exec -it python3_12_8 pip --version
    pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
    ```

2. db:5432, postgres:5432

    Services within the same network are accessible by the service name and port.

3. 104,802; 198,924; 109,603; 27,678; 35,189

    ```psql
    taxi=# select trip_cat, count(trip_cat) from (select case
       when trip_distance <= 1 then 0
       when trip_distance <= 3 then 1
       when trip_distance <= 7 then 3
       when trip_distance <= 10 then 7
       else 10
       end as trip_cat from green_trip
       where lpep_pickup_datetime >= '2019-10-01' and lpep_dropoff_datetime < '2019-11-01')
       as td
       group by trip_cat
       order by trip_cat
       ;
     trip_cat | count
    ----------+--------
            0 | 104802
            1 | 198924
            3 | 109603
            7 |  27678
           10 |  35189
    (5 rows)
    ```

4. 2019-10-31

    ```psql
    taxi=# select lpep_pickup_datetime, trip_distance from green_trip order by trip_distance desc limit 5;
    lpep_pickup_datetime | trip_distance
    ----------------------+---------------
    2019-10-31 23:23:41  |        515.89
    2019-10-11 20:34:21  |         95.78
    2019-10-26 03:02:39  |         91.56
    2019-10-24 10:59:58  |         90.75
    2019-10-05 16:42:04  |         85.23
    (5 rows)
    ```

5. East Harlem North, East Harlem South, Morningside Heights (74, 75, 166)

    ```psql
    taxi=# select pulocationid, zone, sum(total_amount)
     from green_trip
     left JOIN taxi_zone ON green_trip.pulocationid = taxi_zone.locationid
     where lpep_pickup_datetime::date = '2019-10-18'
     group by pulocationid, zone
     having sum(total_amount) > 13000;

      pulocationid |        zone         |   sum
     --------------+---------------------+----------
                74 | East Harlem North   | 18686.68
                75 | East Harlem South   | 16797.26
               166 | Morningside Heights | 13029.79
    (3 rows)
    ```

6. JFK Airport

    ```psql
    taxi=#  SELECT t.tip_amount, dozone.zone
     FROM green_trip t
     INNER JOIN taxi_zone puzone ON t.pulocationid = puzone.locationid
     INNER JOIN taxi_zone dozone ON t.dolocationid = dozone.locationid
     WHERE puzone.zone = 'East Harlem North' AND
         t.lpep_pickup_datetime >= '2019-10-01' AND
         t.lpep_pickup_datetime < '2019-11-01'
     ORDER BY tip_amount DESC
     LIMIT 5;

      tip_amount |       zone
     ------------+-------------------
            87.3 | JFK Airport
           80.88 | Yorkville West
              40 | East Harlem North
              35 | East Harlem North
           26.45 | Newark Airport
    (5 rows)
    ```

7. `terraform init`, `terraform apply -auto-approve`, `terraform destroy`

    - `terraform init`: initializes & configures the backend, installs plugins/providers, and checks out an existing configuration from a version control
    - `terraform apply`: Asks for approval to the proposed plan, and applies changes to cloud
        - `-auto-approve`: Skip interactive approval of plan before applying.
    - `terraform destroy`: Destroy Terraform-managed infrastructure.
