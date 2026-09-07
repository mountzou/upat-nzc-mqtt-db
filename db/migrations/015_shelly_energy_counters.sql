-- Additive only: apply before deploying the counter-aware Shelly ingestor.
-- No changes to existing hourly values or to telemetry timestamp semantics.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';
CREATE TABLE IF NOT EXISTS shelly_energy_counters (
    device_id TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('a','b','c','total')),
    observed_at TIMESTAMPTZ NOT NULL,
    energy_wh DOUBLE PRECISION NOT NULL CHECK (energy_wh >= 0 AND energy_wh < 'Infinity'::float8),
    returned_energy_wh DOUBLE PRECISION CHECK (returned_energy_wh >= 0 AND returned_energy_wh < 'Infinity'::float8),
    counter_kind TEXT NOT NULL CHECK (counter_kind IN ('import','absolute')),
    source_component TEXT NOT NULL,
    timestamp_basis TEXT NOT NULL DEFAULT 'received_at' CHECK (timestamp_basis = 'received_at'),
    PRIMARY KEY (device_id, channel, observed_at)
);

COMMIT;
