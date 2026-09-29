-- Preserve both occurrences of a repeated local forecast hour.
-- Prepare in a separately reviewed release before the new collector/API images.
-- Keep the legacy unique key so the old collector still works before cutover.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE public.weather_hourly_forecasts
    ADD COLUMN IF NOT EXISTS forecast_instant TIMESTAMPTZ;

-- A legacy local timestamp during a DST gap or fold cannot be mapped to one
-- certain instant. Refuse that backfill instead of inventing a source hour.
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
        RAISE EXCEPTION 'Ambiguous legacy forecast local hour; reconcile before backfill';
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
    ) THEN
        ALTER TABLE public.weather_hourly_forecasts
            ADD CONSTRAINT weather_hourly_forecasts_instant_key
            UNIQUE (source, latitude, longitude, forecast_instant);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_weather_hourly_forecasts_instant
    ON public.weather_hourly_forecasts (forecast_instant ASC);

COMMIT;
