# Shared Shelly sequence detached, 2026-10-05

The shared ID sequence was detached from `public.shelly_measurements.id` with
`OWNED BY NONE`. This removes the sequence's automatic ownership dependency on
the legacy table while preserving the existing `public` name used by the compact
writer. Direct ownership by `shelly_compact.measurements.id` would require the
sequence and table to be in the same schema.

## Applied change

- SQL source commit: `b896eff`.
- Source: `db/maintenance/detach_shelly_sequence.sql`.
- Target: existing `iot_postgres` container, `iot_db`, PostgreSQL 15.17.
- The guarded transaction returned `BEGIN`, both guard checks, `ALTER SEQUENCE`,
  and `COMMIT`; command exit status was 0.
- Lock timeout: 1 second; statement timeout: 5 seconds.

```sql
ALTER SEQUENCE public.shelly_measurements_id_seq OWNED BY NONE;
```

## Verification

Read-only checks before and after the transaction confirmed:

- The sequence remained OID **16445**, named
  `public.shelly_measurements_id_seq`, with owner `postgres` and unchanged ACL.
- The automatic ownership dependency on the legacy table was absent afterward.
- Both existing `id` defaults still referenced the same sequence.
- Start 1, increment 1, minimum 1, maximum 2147483647, cache 1 and no-cycle
  settings remained unchanged. No `setval`, `nextval` probe, restart or schema
  move was included in the maintenance SQL.
- The observed sequence value and latest committed compact ID advanced from
  **95965867** (12:18:14 Athens) to **95966755** (12:21:00 Athens); these are dated
  observations, not current counters.
- The legacy table remained OID **16446**, with latest ID **86962336** and event
  time 2026-09-16 21:43:10.676005 Europe/Athens.
- API reads and ingestor writes remained in `compact` mode.
- PostgreSQL, Shelly ingestor and API remained running with unchanged startup
  timestamps and restart counts of 0. PostgreSQL postmaster start remained
  2026-09-17 10:03:22.421025 Europe/Athens.
- The API and ingestor logs contained no matching error/exception/traceback/fatal,
  duplicate-key, permission-denied or missing-relation lines in the inspected
  five-minute window.

This receipt records an already-applied operation. It does not authorize rerunning
the SQL or deleting the legacy table. The September migration runbook and stage
reports retain their historical sequence-ownership descriptions.
