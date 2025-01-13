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

2. db:5432

   Services within the same network are accessible by the service name and port.

3. 104,838; 199,013; 109,645; 27,688; 35,202

   ```psql
   taxi=# select trip_cat, count(trip_cat) from (select case
   when trip_distance <= 1 then 0
   when trip_distance <= 3 then 1
   when trip_distance <= 7 then 3
   when trip_distance <= 10 then 7
   else 10
   end as trip_cat from green_taxi_trips) as td group by trip_cat
   ;
    trip_cat | count
   ----------+--------
           0 | 104838
           1 | 199013
           3 | 109645
           7 |  27688
          10 |  35202
   (5 rows)
   ```

4. 2019-10-31

   ```psql
   taxi=# select lpep_pickup_datetime, trip_distance from green_taxi_trips order by trip_distance desc limit 5;
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
   taxi=# select "PULocationID", sum(total_amount) from green_taxi_trips where lpep_pickup_datetime like '2019-10-18%' group by "PULocationID" having sum(total_amount) > 13000;
    PULocationID |        sum
   --------------+--------------------
              74 |  18686.68000000008
              75 | 16797.260000000057
             166 | 13029.790000000032
   (3 rows)
   ```

   I should join, but I didn't load the zone lookup yet.

6. JFK Airport

   ```psql
   taxi=#  select "DOLocationID", tip_amount from green_taxi_trips where lpep_pickup_datetime like '2019-10-%' and "PULocationID" = 74 order by tip_amount desc limit 5;
   DOLocationID | tip_amount
   --------------+------------
           132 |       87.3
           263 |      80.88
           74 |         40
           74 |         35
               1 |      26.45
   (5 rows)
   ```

7. `terraform init`, `terraform apply -auto-approve`, `terraform destroy`

   - `terraform init`: initializes & configures the backend, installs plugins/providers, and checks out an existing configuration from a version control
   - `terraform apply`: Asks for approval to the proposed plan, and applies changes to cloud
     - `-auto-approve`: Skip interactive approval of plan before applying.
   - `terraform destroy`: Destroy Terraform-managed infrastructure.
