# Module 4 solutions

The solutions are available at [solutions.md](./solutions.md)

## Load data into Bigquery

1. setup bigquery with terraform: `terraform apply`
2. add bucket name to `.env`: `echo "BUCKET_NAME=$(terraform output bucket_name)" > .env`
3. Load data into GCS: `python load_to_gcs.py`
4. run the dbt models: `cd dbt-hw4 && dbt run --vars '{is_test_run: false}'`
