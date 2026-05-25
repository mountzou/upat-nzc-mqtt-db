CREATE TABLE IF NOT EXISTS weather_hourly_forecasts (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'open-meteo',
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    timezone TEXT NOT NULL,
    forecast_timestamp TIMESTAMP NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_hour SMALLINT NOT NULL CHECK (forecast_hour >= 0 AND forecast_hour <= 23),
    temperature_2m_c NUMERIC,
    dew_point_2m_c NUMERIC,
    relative_humidity_2m_percent NUMERIC,
    surface_pressure_hpa NUMERIC,
    shortwave_radiation_w_m2 NUMERIC,
    direct_normal_irradiance_w_m2 NUMERIC,
    diffuse_radiation_w_m2 NUMERIC,
    wind_direction_10m_degrees NUMERIC,
    wind_speed_10m_ms NUMERIC,
    weather_code INTEGER,
    snow_depth_m NUMERIC,
    precipitation_mm NUMERIC,
    cloud_cover_percent NUMERIC,
    raw_values JSONB,
    raw_request JSONB,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (source, latitude, longitude, forecast_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_weather_hourly_forecasts_time
    ON weather_hourly_forecasts (forecast_timestamp ASC);

CREATE INDEX IF NOT EXISTS idx_weather_hourly_forecasts_date_time
    ON weather_hourly_forecasts (forecast_date DESC, forecast_timestamp ASC);
