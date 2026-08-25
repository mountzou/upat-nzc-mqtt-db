-- Additive schema for FusionSolar actual telemetry.
-- This migration intentionally does not alter forecast/shadow tables and does
-- not install extensions, triggers, partitioning, or scheduled jobs.

CREATE TABLE IF NOT EXISTS pv_plants (
    id BIGSERIAL PRIMARY KEY,
    site_key TEXT NOT NULL UNIQUE CHECK (BTRIM(site_key) <> ''),
    provider TEXT NOT NULL DEFAULT 'huawei_fusionsolar'
        CHECK (BTRIM(provider) <> ''),
    provider_plant_dn TEXT NOT NULL CHECK (BTRIM(provider_plant_dn) <> ''),
    name TEXT,
    timezone TEXT NOT NULL DEFAULT 'Europe/Athens'
        CHECK (BTRIM(timezone) <> ''),
    installed_capacity_kw NUMERIC(10, 3)
        CHECK (installed_capacity_kw IS NULL OR installed_capacity_kw >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
        CHECK (JSONB_TYPEOF(metadata) = 'object'),
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (provider, provider_plant_dn),
    CHECK (
        first_seen_at IS NULL
        OR last_seen_at IS NULL
        OR first_seen_at <= last_seen_at
    )
);


CREATE TABLE IF NOT EXISTS pv_devices (
    id BIGSERIAL PRIMARY KEY,
    plant_id BIGINT NOT NULL REFERENCES pv_plants(id) ON DELETE RESTRICT,
    provider_device_id TEXT NOT NULL CHECK (BTRIM(provider_device_id) <> ''),
    provider_device_dn TEXT,
    device_type_code INTEGER NOT NULL,
    device_role TEXT NOT NULL
        CHECK (device_role IN ('inverter', 'grid_meter', 'logger', 'other')),
    name TEXT,
    model TEXT,
    software_version TEXT,
    installed_capacity_kw NUMERIC(10, 3)
        CHECK (installed_capacity_kw IS NULL OR installed_capacity_kw >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
        CHECK (JSONB_TYPEOF(metadata) = 'object'),
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (plant_id, provider_device_id),
    UNIQUE (plant_id, provider_device_dn),
    CHECK (
        first_seen_at IS NULL
        OR last_seen_at IS NULL
        OR first_seen_at <= last_seen_at
    )
);


CREATE TABLE IF NOT EXISTS pv_ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    plant_id BIGINT NOT NULL REFERENCES pv_plants(id) ON DELETE RESTRICT,
    run_key TEXT NOT NULL CHECK (BTRIM(run_key) <> ''),
    trigger_kind TEXT NOT NULL
        CHECK (trigger_kind IN ('scheduled', 'manual', 'backfill', 'migration')),
    source_kind TEXT NOT NULL
        CHECK (source_kind IN ('fusion_live', 'firestore_legacy', 'manual_repair')),
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'partial', 'failed', 'rate_limited')),
    request_start_date DATE NOT NULL,
    request_end_date DATE NOT NULL,
    request_start_at TIMESTAMPTZ NOT NULL,
    request_end_at TIMESTAMPTZ NOT NULL,
    request_timezone TEXT NOT NULL DEFAULT 'Europe/Athens'
        CHECK (BTRIM(request_timezone) <> ''),
    lookback_days SMALLINT NOT NULL CHECK (lookback_days BETWEEN 1 AND 3),
    expected_device_count INTEGER NOT NULL DEFAULT 0
        CHECK (expected_device_count >= 0),
    returned_device_count INTEGER NOT NULL DEFAULT 0
        CHECK (returned_device_count >= 0),
    device_reading_count INTEGER NOT NULL DEFAULT 0
        CHECK (device_reading_count >= 0),
    plant_reading_count INTEGER NOT NULL DEFAULT 0
        CHECK (plant_reading_count >= 0),
    inserted_count INTEGER NOT NULL DEFAULT 0 CHECK (inserted_count >= 0),
    updated_count INTEGER NOT NULL DEFAULT 0 CHECK (updated_count >= 0),
    rejected_count INTEGER NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    call_summary JSONB NOT NULL DEFAULT '[]'::JSONB
        CHECK (JSONB_TYPEOF(call_summary) = 'array'),
    code_version TEXT,
    error_text TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (request_start_date <= request_end_date),
    CHECK (request_start_at <= request_end_at),
    CHECK (
        (status = 'running' AND completed_at IS NULL)
        OR (status <> 'running' AND completed_at IS NOT NULL)
    ),
    CHECK (
        status NOT IN ('failed', 'rate_limited')
        OR BTRIM(COALESCE(error_text, '')) <> ''
    )
);

CREATE INDEX IF NOT EXISTS idx_pv_ingestion_runs_plant_started
    ON pv_ingestion_runs (plant_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pv_ingestion_runs_run_key
    ON pv_ingestion_runs (run_key, started_at DESC);


CREATE TABLE IF NOT EXISTS pv_source_state (
    source_key TEXT PRIMARY KEY CHECK (BTRIM(source_key) <> ''),
    provider TEXT NOT NULL DEFAULT 'huawei_fusionsolar'
        CHECK (BTRIM(provider) <> ''),
    circuit_state TEXT NOT NULL DEFAULT 'closed'
        CHECK (circuit_state IN ('closed', 'open', 'half_open')),
    blocked_until TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0
        CHECK (consecutive_failures >= 0),
    last_fail_code INTEGER,
    sanitized_last_error TEXT,
    last_login_success_at TIMESTAMPTZ,
    last_successful_ingestion_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS pv_device_readings_5m (
    device_id BIGINT NOT NULL REFERENCES pv_devices(id) ON DELETE RESTRICT,
    observed_at TIMESTAMPTZ NOT NULL,
    local_date DATE NOT NULL,
    source_run_id BIGINT NOT NULL
        REFERENCES pv_ingestion_runs(id) ON DELETE RESTRICT,
    source_kind TEXT NOT NULL
        CHECK (source_kind IN ('fusion_live', 'manual_repair')),
    provider_collect_time_ms BIGINT NOT NULL
        CHECK (provider_collect_time_ms >= 0),
    active_power_kw DOUBLE PRECISION,
    reactive_power_kvar DOUBLE PRECISION,
    mppt_power_kw DOUBLE PRECISION,
    power_factor DOUBLE PRECISION,
    efficiency_percent DOUBLE PRECISION,
    temperature_c DOUBLE PRECISION,
    grid_frequency_hz DOUBLE PRECISION,
    inverter_state INTEGER,
    day_energy_kwh DOUBLE PRECISION
        CHECK (day_energy_kwh IS NULL OR day_energy_kwh >= 0),
    total_energy_kwh DOUBLE PRECISION
        CHECK (total_energy_kwh IS NULL OR total_energy_kwh >= 0),
    extra_kpis JSONB NOT NULL DEFAULT '{}'::JSONB
        CHECK (JSONB_TYPEOF(extra_kpis) = 'object'),
    quality_flags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (device_id, observed_at),
    CHECK (
        EXTRACT(SECOND FROM observed_at) = 0
        AND MOD(EXTRACT(MINUTE FROM observed_at)::INTEGER, 5) = 0
    )
);

CREATE INDEX IF NOT EXISTS idx_pv_device_readings_observed
    ON pv_device_readings_5m (observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_pv_device_readings_source_run
    ON pv_device_readings_5m (source_run_id);


CREATE TABLE IF NOT EXISTS pv_plant_readings_5m (
    plant_id BIGINT NOT NULL REFERENCES pv_plants(id) ON DELETE RESTRICT,
    observed_at TIMESTAMPTZ NOT NULL,
    local_date DATE NOT NULL,
    source_run_id BIGINT NOT NULL
        REFERENCES pv_ingestion_runs(id) ON DELETE RESTRICT,
    source_kind TEXT NOT NULL
        CHECK (
            source_kind IN (
                'fusion_live_device_derived',
                'firestore_legacy',
                'manual_repair'
            )
        ),
    provider_collect_time_ms BIGINT
        CHECK (
            provider_collect_time_ms IS NULL
            OR provider_collect_time_ms >= 0
        ),
    aggregation_version TEXT,
    active_power_kw DOUBLE PRECISION,
    reactive_power_kvar DOUBLE PRECISION,
    mppt_power_kw DOUBLE PRECISION,
    power_factor DOUBLE PRECISION,
    reporting_device_count SMALLINT
        CHECK (reporting_device_count IS NULL OR reporting_device_count >= 0),
    expected_device_count SMALLINT
        CHECK (expected_device_count IS NULL OR expected_device_count >= 0),
    reporting_device_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    missing_device_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    quality_status TEXT NOT NULL
        CHECK (quality_status IN ('complete', 'partial', 'night', 'unknown', 'invalid')),
    quality_flags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    source_reference JSONB NOT NULL DEFAULT '{}'::JSONB
        CHECK (JSONB_TYPEOF(source_reference) = 'object'),
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (plant_id, observed_at),
    CHECK (
        reporting_device_count IS NULL
        OR expected_device_count IS NULL
        OR reporting_device_count <= expected_device_count
    ),
    CHECK (
        EXTRACT(SECOND FROM observed_at) = 0
        AND MOD(EXTRACT(MINUTE FROM observed_at)::INTEGER, 5) = 0
    )
);

CREATE INDEX IF NOT EXISTS idx_pv_plant_readings_source_run
    ON pv_plant_readings_5m (source_run_id);
