CREATE TABLE devices (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    device_id TEXT NOT NULL,
    dev_eui TEXT,
    name TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (source, device_id),
    UNIQUE (source, dev_eui)
);

CREATE TABLE raw_messages (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    device_id TEXT NOT NULL,
    topic TEXT,
    payload JSONB NOT NULL,
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP DEFAULT NOW()
);

CREATE TABLE measurements (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT,
    event_time TIMESTAMP
);
