import os
import tempfile
from itertools import product
from concurrent.futures import ThreadPoolExecutor

import requests
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

TABLE_NAMES = ["green", "yellow", "fhv"]
MONTHS = range(1, 13)
YEAR_FHV = (2019,)
YEAR_GY = (2019, 2020)


def transfer_file(name: str, year: int, month: int):
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{name}/{name}_tripdata_{year}-{month:02d}.csv.gz"
    storage_client = storage.Client()
    bucket = storage_client.bucket(os.environ["BUCKET_NAME"])
    file_name = f"{name}/{name}_tripdata_{year}-{month}.csv.gz"
    blob = bucket.blob(file_name)
    if blob.exists():
        print(blob.name, "already exists. Skipping download")
        return

    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with tempfile.TemporaryFile() as file:
            for chunk in response.iter_content(1024):
                file.write(chunk)
            blob.upload_from_file(file, rewind=True)
    print("Uploaded to", blob.self_link)


if __name__ == "__main__":
    with ThreadPoolExecutor() as executor:
        futures = []
        for table_name, month in product(TABLE_NAMES, MONTHS):
            years = YEAR_FHV if table_name == "fhv" else YEAR_GY
            for year in years:
                futures.append(executor.submit(transfer_file, table_name, year, month))
        for future in futures:
            future.result()
