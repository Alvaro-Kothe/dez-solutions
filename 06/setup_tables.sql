CREATE TABLE IF NOT EXISTS processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER
);

CREATE TABLE IF NOT EXISTS taxi_events (
    session_start TIMESTAMP(3),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    num_trips BIGINT
);
