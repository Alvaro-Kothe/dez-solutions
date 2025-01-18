import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

default_args = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=60),
    "conn_id": "taxi_db",
}


@dag(
    default_args=default_args,
    description="Import Yellow and Green Taxi Data from GitHub",
    schedule="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 7, 31),
    catchup=False,
    tags=["data-engineering"],
)
def import_taxi_data():
    create_inserted_table = SQLExecuteQueryOperator(
        task_id="create_inserted_table",
        sql="sql/inserted_schema.sql",
    )

    @task.short_circuit()
    def check_if_not_imported(
        color: str, logical_date: pendulum.DateTime | None = None, conn_id=None
    ):
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        check_existence_query = """
            SELECT 1
            FROM inserted_data
            WHERE color = %s AND year = %s AND month = %s
            ;
        """
        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    check_existence_query,
                    (color, logical_date.year, logical_date.month),
                )
                not_imported = cur.fetchone() is None

        if not_imported:
            logger.info(
                f"Data for {color}, {logical_date} not found. Proceeding to import."
            )
        else:
            logger.info(
                f"Data for {color}, {logical_date} already exists. Skipping import."
            )

        return not_imported

    @task()
    def fetch_and_import_data(
        color: str, logical_date: pendulum.DateTime | None = None, conn_id=None
    ):
        import gzip
        import tempfile

        import requests

        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        table = f"{color}_trip"
        mark_fetched_query = """
            INSERT INTO inserted_data (insertedid, color, year, month, url)
            VALUES (DEFAULT, %s, %s, %s, %s)
            RETURNING insertedid
            ;
        """
        create_temp_table_query = f"""
            CREATE TEMP TABLE trip_temp (LIKE {table});
            ALTER TABLE trip_temp DROP insertedid;
        """

        year = logical_date.year
        month = logical_date.month
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year:04d}-{month:02d}.csv.gz"
        query_import_csv = f"COPY trip_temp FROM STDIN WITH (FORMAT CSV, HEADER TRUE);"
        query_import_from_temp = f"""
            INSERT INTO {table}
            SELECT %s, * FROM trip_temp;
        """

        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    with tempfile.TemporaryFile() as tmp_file:
                        response = requests.get(url, stream=True)
                        response.raise_for_status()
                        logger.info(
                            "Downloading data for %s, %4d-%02d from %s",
                            color,
                            year,
                            month,
                            url,
                        )
                        for chunk in response.iter_content(chunk_size=1024):
                            tmp_file.write(chunk)

                        fsize = tmp_file.tell() / (1024.0 * 1024.0)
                        logger.info("Downloaded %.1f MB", fsize)

                        cur.execute(create_temp_table_query)
                        tmp_file.seek(0)
                        with gzip.GzipFile(fileobj=tmp_file) as csv_file:
                            cur.copy_expert(query_import_csv, csv_file)
                            csv_fsize = csv_file.tell() / (1024.0 * 1024.0)
                            logger.info("Inserted %.1f MB", csv_fsize)

                    cur.execute(mark_fetched_query, (color, year, month, url))
                    insertedid = cur.fetchone()[0]
                    cur.execute(query_import_from_temp, (insertedid, ))
                    conn.commit()
                    logger.info(
                        "Data for '%s, %4d-%02d' successfully imported.",
                        color,
                        year,
                        month,
                    )
                except requests.exceptions.RequestException as e:
                    logger.error(
                        "Error downloading data for '%s, %4d-%02d': %s",
                        color,
                        year,
                        month,
                        e,
                    )
                    raise
                except Exception as e:
                    logger.error(
                        "Error importing data for '%s, %4d-%02d': %s",
                        color,
                        year,
                        month,
                        e,
                    )
                    raise

    for color in ["yellow", "green"]:
        with TaskGroup(f"{color}_data_pipeline") as tg:
            create_trip_table = SQLExecuteQueryOperator(
                task_id=f"create_{color}_trip_table",
                sql=f"sql/{color}_trip_schema.sql",
            )
            verify_if_not_imported = check_if_not_imported(color=color)
            import_data = fetch_and_import_data(color=color)
            create_trip_table >> verify_if_not_imported >> import_data

        create_inserted_table >> tg


import_taxi_data()
