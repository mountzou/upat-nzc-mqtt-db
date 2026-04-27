CREATE TABLE IF NOT EXISTS simulation_day_ahead_runs (
    id SERIAL PRIMARY KEY,
    school_id TEXT NOT NULL,
    recording_date DATE NOT NULL,
    request_url TEXT NOT NULL,
    request_path TEXT NOT NULL,
    request_body JSONB NOT NULL,
    http_status INTEGER,
    status TEXT,
    simulation_engine TEXT,
    external_run_id TEXT,
    day_ahead_date DATE,
    requested_rooms INTEGER,
    successful_rooms INTEGER,
    failed_rooms INTEGER,
    facility_kwh NUMERIC,
    equipment_kwh NUMERIC,
    lighting_kwh NUMERIC,
    heating_liters NUMERIC,
    cooling_kwh NUMERIC,
    fans_hvac_kwh NUMERIC,
    success BOOLEAN NOT NULL DEFAULT FALSE,
    error_text TEXT,
    response_json JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_simulation_day_ahead_runs_school_started
    ON simulation_day_ahead_runs (school_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_simulation_day_ahead_runs_school_day
    ON simulation_day_ahead_runs (school_id, day_ahead_date DESC);

CREATE TABLE IF NOT EXISTS simulation_day_ahead_room_results (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES simulation_day_ahead_runs(id) ON DELETE CASCADE,
    school_id TEXT NOT NULL,
    recording_date DATE NOT NULL,
    room_id TEXT NOT NULL,
    room_label TEXT,
    status TEXT NOT NULL,
    error_text TEXT,
    average_air_temperature_c NUMERIC,
    thermal_discomfort_hours NUMERIC,
    facility_kwh NUMERIC,
    equipment_kwh NUMERIC,
    lighting_kwh NUMERIC,
    heating_liters NUMERIC,
    cooling_kwh NUMERIC,
    fans_hvac_kwh NUMERIC,
    raw_result JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (run_id, room_id)
);

CREATE INDEX IF NOT EXISTS idx_simulation_day_ahead_room_results_school_date
    ON simulation_day_ahead_room_results (school_id, recording_date DESC);

CREATE INDEX IF NOT EXISTS idx_simulation_day_ahead_room_results_run_id
    ON simulation_day_ahead_room_results (run_id);
