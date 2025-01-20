# Solutions to module 1

All the solutions are available on [solutions.md](./solutions.md).

To setup the project for the solutions:

1. start a new fresh postgres instance
   (you may use the [docker-compose.yaml](/docker-compose.yaml) from the project root,
   without mounting the volume.
2. Install the python dependencies and run:
   ```console
   $ python scripts/ingest_url_data.py --year 2019 --month 10 --color green
   ```
3. Load the zone lookup schema
   ```console
   $ psql -U postgres -h localhost -d taxi < queries/create_taxi_zone_lookup.sql
   ```
4. Import the zone lookup data
    ```console
    wget -qO- wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv | psql -U postgres -h localhost -d taxi -c "copy taxi_zone from STDIN with (format csv, header true)"
    ```
