CREATE TABLE IF NOT EXISTS upat_rollup_state (
    pipeline_name TEXT PRIMARY KEY,
    last_measurement_id BIGINT NOT NULL CHECK (last_measurement_id >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
