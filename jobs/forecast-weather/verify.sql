-- Required psql variables: start_date, forecast_days, since, latitude,
-- longitude, timezone. Run after the service finishes successfully.
\set ON_ERROR_STOP on
BEGIN READ ONLY;

WITH expected AS (
    SELECT generate_series(
        :'start_date'::date::timestamp,
        :'start_date'::date + (:'forecast_days'::int * interval '1 day') - interval '1 hour',
        interval '1 hour'
    ) AS timestamp
), coverage AS (
    SELECT e.timestamp, w.forecast_timestamp IS NOT NULL AS present,
           COALESCE(
               w.fetched_at >= :'since'::timestamptz
               AND w.updated_at >= :'since'::timestamptz
               AND w.forecast_date = e.timestamp::date
               AND w.forecast_hour = extract(hour FROM e.timestamp)
               AND w.timezone = :'timezone'
               AND w.raw_request->>'start_date' = :'start_date'
               AND w.raw_request->>'end_date' =
                   (:'start_date'::date + :'forecast_days'::int - 1)::text,
               false
           ) AS refreshed
    FROM expected e
    LEFT JOIN weather_hourly_forecasts w
      ON w.source = 'open-meteo'
     AND w.latitude = :'latitude'::numeric
     AND w.longitude = :'longitude'::numeric
     AND w.forecast_timestamp = e.timestamp
)
SELECT count(*) AS expected_hours,
       count(*) FILTER (WHERE present) AS stored_hours,
       count(*) FILTER (WHERE refreshed) AS refreshed_hours,
       count(*) = :'forecast_days'::int * 24
           AND bool_and(present AND refreshed) AS verified
FROM coverage
\gset

\echo expected_hours=:expected_hours stored_hours=:stored_hours refreshed_hours=:refreshed_hours verified=:verified
\if :verified
\else
    DO $$ BEGIN
        RAISE EXCEPTION 'Incomplete, stale or mismatched forecast rows';
    END $$;
\endif
COMMIT;
