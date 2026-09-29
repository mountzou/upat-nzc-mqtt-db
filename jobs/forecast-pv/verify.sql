-- Supply run_id and since from the service invocation; this query is read-only.
BEGIN READ ONLY;
WITH hours AS (
    SELECT h.*,
           (h.forecast_timestamp = h.forecast_date + make_time(h.forecast_hour, 0, 0)) AS valid_time,
           (h.raw_features->>'wind_speed_10m')::numeric = h.wind_speed_10m AS valid_wind
    FROM pv_day_ahead_forecast_hourly h
    WHERE h.run_id = :'run_id'::integer
)
SELECT r.id, r.forecast_date, r.success, r.completed_at,
       r.completed_at >= :'since'::timestamptz AS completed_since_start,
       r.forecast_date = (r.started_at AT TIME ZONE 'Europe/Athens')::date + 1 AS is_day_ahead,
       r.model_artifact, r.features_artifact,
       r.raw_request->>'model_version' AS model_version,
       count(h.id) AS stored_hours,
       count(DISTINCT h.forecast_timestamp) AS distinct_hours,
       bool_and(h.valid_time AND h.forecast_date = r.forecast_date) AS valid_times,
       bool_and(h.predicted_power_kw >= 0) AS nonnegative_power,
       bool_and(h.valid_wind) AS raw_wind_matches,
       bool_and(h.shortwave_radiation_w_m2 >= r.night_ghi_threshold_wm2 OR h.predicted_power_kw = 0) AS night_mask_valid,
       bool_and(h.lag_1h_kw IS NULL) AND r.lag_1h_kw IS NULL AS legacy_lag_null,
       r.daily_energy_kwh,
       sum(h.predicted_power_kw) AS summed_energy_kwh,
       abs(r.daily_energy_kwh - sum(h.predicted_power_kw)) < 0.000001 AS energy_matches
FROM pv_day_ahead_forecast_runs r
LEFT JOIN hours h ON h.run_id = r.id
WHERE r.id = :'run_id'::integer
GROUP BY r.id;
COMMIT;
