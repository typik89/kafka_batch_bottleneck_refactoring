CREATE TABLE wait (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    original_topic TEXT,
    original_key TEXT,
    original_offset BIGINT,
    original_partition BIGINT,
    original_timestamp TIMESTAMP,
    original_headers TEXT,

    wait_expiration_uuid TEXT UNIQUE,
    wait_expiration_time timestamp NOT NULL,

    body TEXT,

    created_at timestamp  DEFAULT NOW()
);