# Legacy Shelly table retired, 2026-10-05

Result: **PASS**. The separately approved operation removed only
`public.shelly_measurements` and its own indexes/TOAST/row-type objects.
The compact data and existing public ID sequence remain operational.

## Applied operation

- Committed SQL source: `b1c9315`, `db/maintenance/retire_shelly_legacy_table.sql`.
- The exact committed SQL was streamed to the existing `iot_postgres`/`iot_db`.
- The transaction returned `LOCK TABLE`, both guard checks, `DROP TABLE` and
  `COMMIT`, with exit status 0. It used `RESTRICT`, a 1-second lock timeout and
  5-second statement timeout; no CASCADE or sequence reset was executed.
- The preceding [complete history and dependency audit](RETIREMENT-CHECK-20261005.md)
  verified all **64,338,303** legacy rows byte-for-byte in compact.

## Object and data verification

- `to_regclass('public.shelly_measurements')` is NULL afterward.
- Its primary-key and two named measurement indexes are absent.
- The sequence remains `public.shelly_measurements_id_seq`, OID **16445**, with
  the same start/increment/minimum/maximum/cache/no-cycle settings.
- The compact table remains OID **121068** and readings view OID **121079**.
  Its `id` default still references the preserved sequence.
- New compact ID advanced from **95973478** before deletion to **95975647**
  afterward; latest observed raw event was **12:47:49.593275 Europe/Athens**.
- Historical boundary IDs **10938671** (April 1) and **86962336** (September 16)
  remain readable directly from the compact view.
- Actual loopback HTTP history requests for both boundary measurements returned
  200 with two minute buckets each. A latest-measurement request returned 200
  with the **12:47** bucket. `/health` returned HTTP 200 and database connected.

## Space and service continuity

- Available filesystem bytes: **51,767,988,224** before,
  **66,306,035,712** after.
- Net available-space gain: **14,538,047,488 bytes**, approximately
  **14.54 GB / 13.54 GiB**.
- Database size: **27,745,107,303** bytes before,
  **13,207,100,775** after; net reduction
  **14,538,006,528** bytes. Ongoing ingestion accounts for the small
  difference from the legacy relation's exact allocated size.
- PostgreSQL, API and Shelly ingestor retained the same container IDs, startup
  timestamps and restart counts of 0. PostgreSQL postmaster start is unchanged.
- No matching error/exception/traceback/fatal, duplicate-key, permission-denied or
  missing-relation log lines were found for API/ingestor in the final 10-minute window.
- No build, redeploy, restart, VACUUM FULL or explicit checkpoint was performed.

Postflight observations were recorded at **12:48:31 Europe/Athens**. These are
operator loopback/data checks, not a browser login test. Development initialization
and legacy-mode source compatibility were outside this deletion's scope; production
continues to use compact mode.

Machine-readable evidence: [LEGACY-RETIRED-20261005.json](LEGACY-RETIRED-20261005.json).
This receipt records an already-applied operation and does not authorize rerunning it.
