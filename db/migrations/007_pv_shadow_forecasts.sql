CREATE TABLE IF NOT EXISTS pv_shadow_forecast_runs (
    id BIGSERIAL PRIMARY KEY,
    source_champion_run_id INTEGER NOT NULL
        REFERENCES pv_day_ahead_forecast_runs(id) ON DELETE RESTRICT,
    forecast_date DATE NOT NULL,
    candidate_id TEXT NOT NULL CHECK (BTRIM(candidate_id) <> ''),
    candidate_version TEXT NOT NULL CHECK (BTRIM(candidate_version) <> ''),
    model_artifact TEXT NOT NULL CHECK (BTRIM(model_artifact) <> ''),
    model_sha256 TEXT NOT NULL
        CHECK (model_sha256 ~ '^[0-9a-f]{64}$'),
    features_artifact TEXT NOT NULL CHECK (BTRIM(features_artifact) <> ''),
    features_sha256 TEXT NOT NULL
        CHECK (features_sha256 ~ '^[0-9a-f]{64}$'),
    manifest_artifact TEXT NOT NULL CHECK (BTRIM(manifest_artifact) <> ''),
    manifest_sha256 TEXT NOT NULL
        CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    input_sha256 TEXT NOT NULL
        CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'failed')),
    daily_energy_kwh NUMERIC
        CHECK (daily_energy_kwh IS NULL OR daily_energy_kwh >= 0),
    error_text TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (source_champion_run_id, candidate_id, candidate_version),
    UNIQUE (id, source_champion_run_id, forecast_date),

    CHECK (
        (status = 'running' AND completed_at IS NULL)
        OR (
            status = 'success'
            AND daily_energy_kwh IS NOT NULL
            AND error_text IS NULL
            AND completed_at IS NOT NULL
        )
        OR (
            status = 'failed'
            AND error_text IS NOT NULL
            AND completed_at IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_pv_shadow_forecast_runs_candidate_started
    ON pv_shadow_forecast_runs (
        candidate_id,
        candidate_version,
        started_at DESC
    );


CREATE TABLE IF NOT EXISTS pv_shadow_forecast_hourly (
    id BIGSERIAL PRIMARY KEY,
    shadow_run_id BIGINT NOT NULL,
    source_champion_run_id INTEGER NOT NULL,
    forecast_timestamp TIMESTAMP NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_hour SMALLINT NOT NULL
        CHECK (forecast_hour >= 0 AND forecast_hour <= 23),
    predicted_power_kw_raw NUMERIC NOT NULL,
    predicted_power_kw NUMERIC NOT NULL CHECK (predicted_power_kw >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (shadow_run_id, source_champion_run_id, forecast_date)
        REFERENCES pv_shadow_forecast_runs(
            id,
            source_champion_run_id,
            forecast_date
        )
        ON DELETE CASCADE,
    FOREIGN KEY (source_champion_run_id, forecast_timestamp)
        REFERENCES pv_day_ahead_forecast_hourly(run_id, forecast_timestamp)
        ON DELETE RESTRICT,

    UNIQUE (shadow_run_id, forecast_timestamp),

    CHECK (forecast_timestamp::date = forecast_date),
    CHECK (EXTRACT(HOUR FROM forecast_timestamp)::SMALLINT = forecast_hour)
);

CREATE INDEX IF NOT EXISTS idx_pv_shadow_forecast_hourly_source
    ON pv_shadow_forecast_hourly (
        source_champion_run_id,
        forecast_timestamp
    );
