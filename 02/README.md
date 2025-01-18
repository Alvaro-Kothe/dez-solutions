# Solutions for Module 2

## Without orchestrator

Use the [ingest_url_data.py](/scripts/ingest_url_data.py) to populate the database.
The script [scripts/ingest_2020.sh](/scripts/ingest_2020.sh)
just calls the python script to fill the data for green and yellow for the whole year of 2020.

## Using Airflow on bare metal

1. Initiate airflow with `airflow standalone`.
2. Copy the files from [dags](./dags) into `~/airflow/dags`.
3. Add connection to the taxi database from this project in `admin -> connections`.
4. backfill with `airflow dags backfill --start-date 2019-01-01 --end-date 2021-07-31 import_taxi_data`

## Using Airflow with docker

With a terminal in this directory do:

1. start the airflow services with: `podman compose up -d`
2. backfill with: `podman compose exec -it airflow-triggerer airflow dags backfill --start-date 2020-01-01 --end-date 2021-07-31 import_taxi_data`

If you don't have `podman`, use `docker`.
