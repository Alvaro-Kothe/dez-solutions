# Solutions Module 4

1. `select * from myproject.my_nyc_tripdata.ext_green_taxi`

    The `{{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}` will
    look at the source name `raw_nyc_tripdata` then at the table `ext_green_taxi`,
    the values in the jinja template will be substituted to `<gcp-project>.<bq-database>.<table>`, which is
    `myproject.my_nyc_tripdata.ext_green_taxi`

2. Update the WHERE clause to `where pickup_datetime >= CURRENT_DATE - INTERVAL {{ var("days_back", env_var("DAYS_BACK", "30")) }} DAY`

    According to the [docs](https://docs.getdbt.com/docs/build/project-variables#variable-precedence), `vars` allows several precedences with CLI arguments above all else.
    To setup the default of 30 days, while allowing to override through cli or project configuration, I would replace the `30 DAY` with `{{ var("days_back", "30") }} DAY`.
    But I also want to setup a default value with an environment variable, which makes me replace the `"30"` with `env_var("DAYS_BACK", "30")`.

3. `dbt run ...` ?

    According to the [`--select` docs](https://docs.getdbt.com/reference/node-selection/syntax#how-does-selection-work),
    the `+` prefixed operator selects a model and all of its ancestors (parents).
    The `+` suffix operator selects the model and all of its children.

    - `dbt run` runs everything ❌

        > By default, `dbt run` executes all of the models in the dependency graph

    - The `--select +models/core/dim_taxi_trips.sql+` will select `fct_taxi_monthly_zone_revenue` because it is a children. ❓
        - This may not run because of `--target prod` because it will throw an error if there is no target named prod,
          so it will not materialize the `fct_taxi_monthly_zone_revenue` table there isn't a `prod` target.
          The main problem is that there is no specification about the profiles.
    - The `--select +models/core/fct_taxi_monthly_zone_revenue.sql` will run the model and all of its parents. ❌
    - `--select +models/core/` will run `fct_taxi_monthly_zone_revenue` because it's located in the `models/core` directory (according to the last item). ❌
    - `--select models/staging/+` will not run `fct_taxi_monthly_zone_revenue`. ✔️

        Before it runs `fct_taxi_monthly_zone_revenue`,
        the pipeline will error because it can't build `dim_taxi_trips`
        since the dependency `dim_zone_lookup` (maybe) won't run.

4. 1,3,4,5 are True

    - True. dbt will raise a compilation error if `DBT_BIGQUERY_TARGET_DATASET` is not set.
      Also, because it's used in the default value of the `else` block, an error will be raised if its not set while running the `else` block,
      even if `DBT_BIGQUERY_STAGING_DATASET` is set.
    - False. Since the macro is being called with `resolve_schema_for('core')` the `else` block will not be evaluated.
    - True. It's the only value
    - True. It follows the rules of the `else` block
    - True. It follows the rules of the `else` block

5. green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

    ```sql
    SELECT * FROM `raw_nyc_tripdata.fct_taxi_trips_quarterly_revenue` WHERE year_quarter LIKE '2020%' ORDER BY yoy
    ```

    ```
    service_type	year_quarter	revenue	previous_year_revenue	yoy
    green	2020/Q2	1544036.31	21498354	-0.928178859
    yellow	2020/Q2	15560725.84	200294863.79	-0.922310909
    green	2020/Q3	2360835.79	17651033.84	-0.866249433
    green	2020/Q4	2441470.26	15680616.87	-0.844300114
    yellow	2020/Q3	41404401.54	186983438.42	-0.778566477
    yellow	2020/Q4	56283852.03	191426671.42	-0.705976959
    green	2020/Q1	11480845.79	26440852.61	-0.565791393
    yellow	2020/Q1	144118830.2	182726442.94	-0.211286402
    ```

6. green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

    ```sql
    SELECT * FROM `raw_nyc_tripdata.fct_taxi_trips_monthly_fare_p95` WHERE year = 2020 AND month = 4
    ```

    ```
    service_type	year	month	p97	p95	p90
    green	2020	4	55.0	45.0	26.5
    yellow	2020	4	31.5	25.5	19.0
    ```

7. LaGuardia Airport, Chinatown, Garment District

    ```sql
    WITH ranked_durations AS (SELECT
        pickup_zone,
        dropoff_zone,
        p90_trip_duration,
        DENSE_RANK() OVER (PARTITION BY pickup_zone ORDER BY p90_trip_duration DESC) AS rank
        FROM `raw_nyc_tripdata.fct_fhv_monthly_zone_traveltime_p90`
        WHERE pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND month = 11
        AND year = 2019)
    SELECT * FROM ranked_durations WHERE rank = 2
    ```

    ```
    pickup_zone	dropoff_zone	p90_trip_duration	rank
    Newark Airport	LaGuardia Airport	7028.8000000000011	2
    Yorkville East	Garment District	13846.0	2
    SoHo	Chinatown	19496.0	2
    ```
