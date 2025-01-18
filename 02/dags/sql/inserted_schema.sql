CREATE TABLE IF NOT EXISTS inserted_data (
    insertedid int GENERATED ALWAYS AS IDENTITY UNIQUE,
    color text NOT NULL,
    year int NOT NULL,
    month int NOT NULL,
    url text,
    CONSTRAINT unique_entry UNIQUE (color, year, month)
)
