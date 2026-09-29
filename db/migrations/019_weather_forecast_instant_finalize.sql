-- Apply only after the old collector writer is stopped and a compatible new
-- collector/API release is ready. The old ON CONFLICT key stops working after
-- this transaction; resume collection only with the new images.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- The old collector may have written NULL instants since migration 018. Refuse
-- ambiguous legacy hours and roll back the entire transaction if any exist.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.weather_hourly_forecasts
        WHERE forecast_instant IS NULL
          AND (
              (forecast_timestamp AT TIME ZONE timezone) AT TIME ZONE timezone
                  <> forecast_timestamp
              OR ((forecast_timestamp AT TIME ZONE timezone) - INTERVAL '1 hour')
                  AT TIME ZONE timezone = forecast_timestamp
              OR ((forecast_timestamp AT TIME ZONE timezone) + INTERVAL '1 hour')
                  AT TIME ZONE timezone = forecast_timestamp
          )
    ) THEN
        RAISE EXCEPTION 'Ambiguous legacy forecast local hour; reconcile before finalization';
    END IF;
END $$;

UPDATE public.weather_hourly_forecasts
SET forecast_instant = forecast_timestamp AT TIME ZONE timezone
WHERE forecast_instant IS NULL;

ALTER TABLE public.weather_hourly_forecasts
    ALTER COLUMN forecast_instant SET NOT NULL;

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
