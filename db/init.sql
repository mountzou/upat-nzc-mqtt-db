CREATE TABLE IF NOT EXISTS upat_devices (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    device_id TEXT NOT NULL,
    dev_eui TEXT,
    name TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (source, device_id),
    UNIQUE (source, dev_eui)
);

CREATE INDEX IF NOT EXISTS idx_upat_devices_source_device_id
    ON upat_devices (source, device_id);

CREATE TABLE IF NOT EXISTS shelly_devices (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    device_id TEXT NOT NULL,
    name TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (source, device_id)
);

CREATE INDEX IF NOT EXISTS idx_shelly_devices_source_device_id
    ON shelly_devices (source, device_id);

CREATE TABLE IF NOT EXISTS upat_raw_messages (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    device_id TEXT NOT NULL,
    topic TEXT,
    payload JSONB NOT NULL,
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_upat_raw_messages_device_id_event_time
    ON upat_raw_messages (device_id, event_time DESC);

CREATE TABLE IF NOT EXISTS upat_measurements (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT,
    event_time TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_upat_measurements_device_metric_event_time
    ON upat_measurements (device_id, metric, event_time DESC);

CREATE TABLE IF NOT EXISTS shelly_raw_messages (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    event_time TIMESTAMP,
    ingestion_time TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_shelly_raw_messages_device_id_event_time
    ON shelly_raw_messages (device_id, event_time DESC);

CREATE TABLE IF NOT EXISTS shelly_measurements (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION,
    unit TEXT,
    event_time TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_shelly_measurements_device_metric_event_time
    ON shelly_measurements (device_id, metric, event_time DESC);
