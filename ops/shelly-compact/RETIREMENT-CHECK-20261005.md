# Shelly legacy-table retirement check, 2026-10-05

Result: **PASS for the inspected data, database dependencies and operational consumers**.
These checks were read-only. `public.shelly_measurements` remains present.
Deletion requires a separate authorization; the proposed guarded SQL has not been run.

## Complete historical-data equality

Both streams were read in the same READ ONLY REPEATABLE READ snapshot
`21072565:21072565:` between 12:30:35 and 12:33:38 Europe/Athens.

- Fields: `id, device_id, metric, value, unit, event_time`.
- Legacy: all rows, ordered by ID.
- Compact: joined to the series dictionary, ID <= **86962336**, ordered by ID.
- Exact row count on each side: **64,338,303**.
- Exact binary COPY bytes on each side: **5,469,048,124**.
- Shared SHA-256: `b728440c4360614dc2d6ca2ea94bdeb4ec46a84db8444c41b9060e331f83842c`.
- Legacy boundary IDs: **10938671–86962336**; corresponding event times
  2026-04-01 00:00:00 through 2026-09-16 21:43:10.676005 Europe/Athens.
- Duration: **183.196 seconds**; values were streamed to hashes without an export file.
- COPY row counting was validated with a two-row probe before the full comparison.

## Dependencies and consumers

- Shared sequence OID **16445** remains in `public`, with no column-ownership dependency.
- Legacy table OID **16446** and compact table OID **121068** remain present.
- Recursive dependency inspection, including the implicit row and array types,
  found only the legacy table's own constraint, defaults, indexes and TOAST objects.
- No foreign keys, non-internal triggers, extension membership or user functions
  containing the legacy table name were found. No active query mentioning the
  legacy table was observed during the preflight snapshot.
- Live API and ingestor files matched the reviewed source hashes. Runtime modes
  are compact reads (`shelly_compact.readings`, `decimal_1`) and compact writes.
- The installed scheduled aggregator image was inspected by streaming
  `docker image save`; no job container was started. Its image is
  `sha256:c16ade3bf7365c427ef44f261d4fe9a5b06ad3b50e485a58cc3caa01b9d7ffa7`.
  The embedded source differs from the current local source, so its actual SQL
  was checked independently: it reads `shelly_energy_counters`/`shelly_devices`
  and writes hourly-energy tables, with no legacy measurements-table reference.
- No direct legacy-table reference appeared in the inspected crontabs or nine
  installed `upat-` service definitions, including the notifications template.

## Continued operation and proposed scope

- The legacy maximum remained **86962336** after the scan. The compact maximum
  advanced to **95971315**, event time **12:35:23.515783 Europe/Athens**.
- API `/health`: HTTP 200, `status=ok`, `database=connected`.
- PostgreSQL, API and Shelly ingestor retained container IDs, startup timestamps
  and restart counts of 0. No matching API/ingestor error lines appeared in the
  inspected 20-minute window. PostgreSQL postmaster start stayed unchanged.
- Legacy table plus indexes occupy **14,538,383,360 bytes** (about 13.54 GiB).
- Proposed source: `db/maintenance/retire_shelly_legacy_table.sql`.
  It checks object IDs, detached sequence ownership and the legacy ID boundary,
  then uses `DROP TABLE public.shelly_measurements RESTRICT`, followed by retained
  object checks in the same transaction. Lock timeout is 1 second and statement
  timeout is 5 seconds. Unexpected catalog dependencies abort the operation.
- The proposal contains no CASCADE, sequence reset, compact-data change or
  container lifecycle command. It remains **unapplied**.

Evidence JSON: [RETIREMENT-CHECK-20261005.json](RETIREMENT-CHECK-20261005.json).
The consumer audit covers inspected runtime code and scheduled configuration;
it cannot prove the absence of unobserved external/manual future consumers.
The legacy-mode fallback still exists in source, while production modes are compact.
Approval of this read-only audit does not authorize dropping the table.
