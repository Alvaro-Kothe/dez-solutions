import csv
import gzip
import io
import json
import tempfile
import time

import requests
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


server = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=[server], value_serializer=json_serializer)

data_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
topic_name = "green-trips"

with requests.get(data_url, stream=True) as response:
    response.raise_for_status()
    with tempfile.TemporaryFile() as file:
        for chunk in response.iter_content(1024):
            file.write(chunk)
        file.seek(0)
        with gzip.GzipFile(fileobj=file) as gz:
            with io.TextIOWrapper(gz, encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)
                t0 = time.time()
                for row in reader:
                    message = {
                        "lpep_pickup_datetime": row["lpep_pickup_datetime"],
                        "lpep_dropoff_datetime": row["lpep_dropoff_datetime"],
                        "PULocationID": int(row["PULocationID"]),
                        "DOLocationID": int(row["DOLocationID"]),
                        "passenger_count": int(row["passenger_count"])
                        if row["passenger_count"]
                        else None,
                        "trip_distance": float(row["trip_distance"]),
                        "tip_amount": float(row["tip_amount"]),
                    }
                    producer.send(topic_name, value=message)

producer.flush()

t1 = time.time()
print(f"took {t1 - t0} seconds")

producer.close()
