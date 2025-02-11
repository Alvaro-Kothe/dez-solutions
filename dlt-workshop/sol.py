import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb
import pathlib

# Q2
BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


# Define API source
@dlt.resource(name="rides")
def ny_taxi():
    # automatic pagination
    client = RESTClient(
        base_url=BASE_URL, paginator=PageNumberPaginator(base_page=1, total_path=None)
    )

    for page in client.paginate(
        "data_engineering_zoomcamp_api"
    ):  # <--- API endpoint for retrieving taxi ride data
        yield page  # <--- yield data to manage memory


pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline", destination="duckdb", dataset_name="ny_taxi_data"
)

db_file = pathlib.Path(f"{pipeline.pipeline_name}.duckdb")

if not db_file.exists():
    load_info = pipeline.run(ny_taxi)
    print(load_info)

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(db_file)

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

print("Q2")
print(conn.sql("SHOW TABLES"))

# Q3
print("Q3")
print(conn.sql("SELECT COUNT(*) FROM rides"))

# Q4
print("Q4")
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)
