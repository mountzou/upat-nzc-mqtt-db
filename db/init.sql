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

CREATE INDEX IF NOT EXISTS idx_shelly_energy_device_time
    ON shelly_measurements (device_id, event_time)
    WHERE metric IN ('a_act_power', 'b_act_power', 'c_act_power');

CREATE INDEX IF NOT EXISTS idx_shelly_energy_covering
    ON shelly_measurements (device_id, event_time, metric)
    INCLUDE (value)
    WHERE metric IN ('a_act_power', 'b_act_power', 'c_act_power');

CREATE TABLE shelly_plug_hourly_energy (
    device_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,

    energy_wh DOUBLE PRECISION,

    is_working_day SMALLINT NOT NULL,
    is_working_hour SMALLINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (device_id, window_start, window_end)
);

CREATE TABLE shelly_pro3em_hourly_energy (
    device_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,

    a_energy_wh DOUBLE PRECISION,
    b_energy_wh DOUBLE PRECISION,
    c_energy_wh DOUBLE PRECISION,
    total_energy_wh DOUBLE PRECISION,

    is_working_day SMALLINT NOT NULL,
    is_working_hour SMALLINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (device_id, window_start, window_end)
);

CREATE INDEX idx_plug_window_time
ON shelly_plug_hourly_energy (window_start, window_end);

CREATE INDEX idx_pro3em_window_time
ON shelly_pro3em_hourly_energy (window_start, window_end);

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
