-- Align weather_hourly_forecasts column names with Open-Meteo hourly variables.
-- ALTER TABLE ... RENAME COLUMN preserves existing values and column types.

DO $$
DECLARE
    rename_pair RECORD;
    old_exists BOOLEAN;
    new_exists BOOLEAN;
BEGIN
    IF TO_REGCLASS('public.weather_hourly_forecasts') IS NULL THEN
        RAISE EXCEPTION 'weather_hourly_forecasts does not exist';
    END IF;

    FOR rename_pair IN
        SELECT *
        FROM (
            VALUES
                ('temperature_2m_c', 'temperature_2m'),
                ('dew_point_2m_c', 'dew_point_2m'),
                ('relative_humidity_2m_percent', 'relative_humidity_2m'),
                ('surface_pressure_hpa', 'surface_pressure'),
                ('shortwave_radiation_w_m2', 'shortwave_radiation'),
                ('direct_normal_irradiance_w_m2', 'direct_normal_irradiance'),
                ('diffuse_radiation_w_m2', 'diffuse_radiation'),
                ('wind_direction_10m_degrees', 'wind_direction_10m'),
                ('wind_speed_10m_ms', 'wind_speed_10m'),
                ('snow_depth_m', 'snow_depth'),
                ('precipitation_mm', 'precipitation'),
                ('cloud_cover_percent', 'cloud_cover')
        ) AS column_names(old_name, new_name)
    LOOP
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'weather_hourly_forecasts'
              AND column_name = rename_pair.old_name
        ) INTO old_exists;

        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'weather_hourly_forecasts'
              AND column_name = rename_pair.new_name
        ) INTO new_exists;

        IF old_exists AND new_exists THEN
            RAISE EXCEPTION
                'weather_hourly_forecasts contains both % and %',
                rename_pair.old_name,
                rename_pair.new_name;
        ELSIF old_exists THEN
            EXECUTE FORMAT(
                'ALTER TABLE public.weather_hourly_forecasts RENAME COLUMN %I TO %I',
                rename_pair.old_name,
                rename_pair.new_name
            );
        ELSIF NOT new_exists THEN
            RAISE EXCEPTION
                'weather_hourly_forecasts contains neither % nor %',
                rename_pair.old_name,
                rename_pair.new_name;
        END IF;
    END LOOP;
END
$$;
