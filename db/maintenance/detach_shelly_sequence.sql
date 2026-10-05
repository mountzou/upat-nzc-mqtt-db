-- Manual, explicitly approved operation; not a startup migration.
-- Detach the legacy-table dependency without moving the shared sequence.
-- Preserve public.shelly_measurements_id_seq and its existing value/settings.
BEGIN;
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';
DO $guard$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_depend d JOIN pg_attribute a
      ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
    WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
      AND d.objid = 'public.shelly_measurements_id_seq'::regclass
      AND d.deptype = 'a' AND d.refobjid = 'public.shelly_measurements'::regclass
      AND a.attname = 'id'
  ) THEN
    RAISE EXCEPTION 'Unexpected sequence ownership; no change applied';
  END IF;
END;
$guard$;
ALTER SEQUENCE public.shelly_measurements_id_seq OWNED BY NONE;
DO $guard$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_depend d
    WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass
      AND d.objid = 'public.shelly_measurements_id_seq'::regclass
      AND d.deptype IN ('a', 'i')
  ) THEN
    RAISE EXCEPTION 'Sequence ownership remains; rolling back';
  END IF;
END;
$guard$;
COMMIT;
