CREATE TABLE IF NOT EXISTS simulation_runs (
    id SERIAL PRIMARY KEY,
    school_id TEXT NOT NULL,
    request_url TEXT NOT NULL,
    request_path TEXT NOT NULL,
    http_status INTEGER,
    success BOOLEAN NOT NULL DEFAULT FALSE,
    error_text TEXT,
    response_json JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_simulation_runs_school_started
    ON simulation_runs (school_id, started_at DESC);

CREATE TABLE IF NOT EXISTS simulation_room_recordings (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES simulation_runs(id) ON DELETE CASCADE,
    school_id TEXT NOT NULL,
    recording_date DATE NOT NULL,
    room_id TEXT NOT NULL,
    label TEXT,
    physical_instance_count INTEGER,
    idf_file TEXT,
    zone_name TEXT,
    thermostat_type TEXT,
    supports_cooling_setpoint BOOLEAN,
    default_occupancy INTEGER,
    default_heating_setpoint NUMERIC,
    default_cooling_setpoint NUMERIC,
    default_lighting_w_per_m2 NUMERIC,
    default_infiltration_ach NUMERIC,
    raw_item JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (school_id, recording_date, room_id)
);

CREATE INDEX IF NOT EXISTS idx_simulation_room_recordings_school_date
    ON simulation_room_recordings (school_id, recording_date DESC);

CREATE INDEX IF NOT EXISTS idx_simulation_room_recordings_run_id
    ON simulation_room_recordings (run_id);
