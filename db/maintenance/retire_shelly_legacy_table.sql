-- Proposed deletion only. Requires separate approval after the read-only audit.
BEGIN;
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';
LOCK TABLE public.shelly_measurements IN ACCESS EXCLUSIVE MODE;
DO $guard$
BEGIN
  IF 'public.shelly_measurements'::regclass::oid <> 16446
     OR 'public.shelly_measurements_id_seq'::regclass::oid <> 16445
     OR 'shelly_compact.measurements'::regclass::oid <> 121068 THEN
    RAISE EXCEPTION 'Unexpected object identity; no table deletion';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass
      AND d.refclassid='pg_class'::regclass
      AND d.objid='public.shelly_measurements_id_seq'::regclass AND d.deptype IN ('a','i')
  ) THEN
    RAISE EXCEPTION 'Shared sequence has an ownership dependency; no table deletion';
  END IF;
  IF (SELECT id FROM public.shelly_measurements ORDER BY id DESC LIMIT 1) IS DISTINCT FROM 86962336 THEN
    RAISE EXCEPTION 'Legacy history boundary changed since full comparison; no table deletion';
  END IF;
END;
$guard$;
DROP TABLE public.shelly_measurements RESTRICT;
DO $guard$
BEGIN
  IF to_regclass('public.shelly_measurements') IS NOT NULL
     OR to_regclass('public.shelly_measurements_id_seq')::oid IS DISTINCT FROM 16445
     OR to_regclass('shelly_compact.measurements')::oid IS DISTINCT FROM 121068 THEN
    RAISE EXCEPTION 'Unexpected retained-object state; rolling back';
  END IF;
END;
$guard$;
COMMIT;
