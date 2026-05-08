CREATE TABLE IF NOT EXISTS pv_day_ahead_forecast_runs (
    id SERIAL PRIMARY KEY,
    forecast_date DATE NOT NULL,
    latitude NUMERIC,
    longitude NUMERIC,
    forecast_days INTEGER NOT NULL DEFAULT 2,
    lag_1h_kw NUMERIC,
    night_ghi_threshold_wm2 NUMERIC,
    daily_energy_kwh NUMERIC,
    source TEXT NOT NULL DEFAULT 'open-meteo',
    model_artifact TEXT,
    features_artifact TEXT,
    success BOOLEAN NOT NULL DEFAULT FALSE,
    error_text TEXT,
    raw_request JSONB,
    raw_summary JSONB,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pv_day_ahead_forecast_runs_started
    ON pv_day_ahead_forecast_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pv_day_ahead_forecast_runs_date
    ON pv_day_ahead_forecast_runs (forecast_date DESC);

CREATE TABLE IF NOT EXISTS pv_day_ahead_forecast_hourly (
    id SERIAL PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES pv_day_ahead_forecast_runs(id) ON DELETE CASCADE,
    forecast_timestamp TIMESTAMP NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_hour SMALLINT NOT NULL CHECK (forecast_hour >= 0 AND forecast_hour <= 23),
    predicted_power_kw NUMERIC NOT NULL CHECK (predicted_power_kw >= 0),
    shortwave_radiation_w_m2 NUMERIC,
    direct_normal_irradiance_w_m2 NUMERIC,
    diffuse_radiation_w_m2 NUMERIC,
    temperature_2m_c NUMERIC,
    cloud_cover_percent NUMERIC,
    wind_speed_10m NUMERIC,
    lag_1h_kw NUMERIC,
    raw_features JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (run_id, forecast_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_pv_day_ahead_forecast_hourly_run_id
    ON pv_day_ahead_forecast_hourly (run_id);

CREATE INDEX IF NOT EXISTS idx_pv_day_ahead_forecast_hourly_date_time
    ON pv_day_ahead_forecast_hourly (forecast_date DESC, forecast_timestamp ASC);
