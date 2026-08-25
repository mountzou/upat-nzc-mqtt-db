CREATE TABLE IF NOT EXISTS upat_measurements_5min (
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT,
    bucket_start TIMESTAMP NOT NULL,
    value_avg DOUBLE PRECISION,
    value_min DOUBLE PRECISION,
    value_max DOUBLE PRECISION,
    sample_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (device_id, metric, bucket_start)
);

CREATE TABLE IF NOT EXISTS upat_measurements_hourly (
    device_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT,
    bucket_start TIMESTAMP NOT NULL,
    value_avg DOUBLE PRECISION,
    value_min DOUBLE PRECISION,
    value_max DOUBLE PRECISION,
    sample_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (device_id, metric, bucket_start)
);
