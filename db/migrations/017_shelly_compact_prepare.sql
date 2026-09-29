-- Explicit preparation only; no rename, data deletion or change to live readers.
-- Execute once with psql ON_ERROR_STOP, inside a transaction.
SET LOCAL lock_timeout = '1s';
CREATE SCHEMA shelly_compact;
CREATE TABLE shelly_compact.series (
    series_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id text NOT NULL,
    metric text NOT NULL,
    unit text,
    UNIQUE NULLS NOT DISTINCT (device_id, metric, unit)
);
CREATE TABLE shelly_compact.measurements (
    -- Keep the original ID allocator for reversible mixed-mode operation.
    -- Transfer sequence ownership before any future retirement of the old table.
    id integer PRIMARY KEY DEFAULT nextval('public.shelly_measurements_id_seq'::regclass),
    series_id integer NOT NULL REFERENCES shelly_compact.series(series_id),
    value double precision,
    event_time timestamptz
);
-- The covering index is built CONCURRENTLY after historical backfill.
-- This avoids a sparse index from incremental reverse-time insertion.
CREATE VIEW shelly_compact.readings AS
    SELECT m.id, s.device_id, s.metric, m.value, s.unit, m.event_time
    FROM shelly_compact.measurements m JOIN shelly_compact.series s USING (series_id);

-- No UPDATE of existing dictionary rows: avoid per-measurement dictionary bloat.
CREATE FUNCTION shelly_compact.resolve_series(d text, m text, u text)
RETURNS integer LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, shelly_compact AS $$
DECLARE result integer;
BEGIN
    FOR attempt IN 1..3 LOOP
        SELECT series_id INTO result FROM shelly_compact.series
        WHERE device_id=d AND metric=m AND unit IS NOT DISTINCT FROM u;
        IF FOUND THEN RETURN result; END IF;
        INSERT INTO shelly_compact.series(device_id,metric,unit) VALUES(d,m,u)
        ON CONFLICT (device_id,metric,unit) DO NOTHING RETURNING series_id INTO result;
        IF FOUND THEN RETURN result; END IF;
        -- A concurrent committed insertion becomes visible on the next READ COMMITTED command.
    END LOOP;
    RAISE EXCEPTION 'Concurrent series creation: retry the whole transaction' USING ERRCODE='40001';
END $$;
CREATE TABLE shelly_compact.copy_progress (
    direction text PRIMARY KEY CHECK (direction IN ('forward','reverse')),
    high_water integer NOT NULL,
    last_id bigint NOT NULL DEFAULT -2147483649,
    copied_rows bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON SCHEMA shelly_compact FROM PUBLIC;
REVOKE ALL ON FUNCTION shelly_compact.resolve_series(text,text,text) FROM PUBLIC;
-- Current production readers/writer use the existing postgres role. A role change
-- requires explicit GRANTs; do not silently broaden access for new roles.
