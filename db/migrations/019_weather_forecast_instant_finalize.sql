-- Apply only after the new UTC-instant collector and API are verified active.
-- The old collector's ON CONFLICT key stops working after this transaction.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.weather_hourly_forecasts'::regclass
          AND conname = 'weather_hourly_forecasts_instant_key'
          AND contype = 'u'
    ) THEN
        RAISE EXCEPTION 'UTC-instant uniqueness is not installed';
    END IF;
END $$;

ALTER TABLE public.weather_hourly_forecasts
    DROP CONSTRAINT IF EXISTS weather_hourly_forecasts_source_latitude_longitude_forecast_key;

COMMIT;
