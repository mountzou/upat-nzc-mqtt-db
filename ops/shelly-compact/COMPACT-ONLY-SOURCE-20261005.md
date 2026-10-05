# Compact-only Shelly source and images, 2026-10-05

The requested source cleanup is complete. Shelly reads always use
`shelly_compact.readings` and one-decimal numeric averages; UPAT retains AVG.
The ingestor always allocates from the existing independent public sequence and
inserts compact rows inside the caller's transaction. Legacy/dual branches,
mode arguments and storage/write/rounding environment selectors were removed.

Fresh initialization now creates the independent integer sequence, compact
series/measurements, covering index, dictionary resolver and readings view,
without creating the old table or its indexes. Schema/function privileges match
the established compact contract. Migration 014 skips only an absent retired
Shelly table, so both old migration fixtures and new initialization work.

## Verification

- Focused writer/counter/Compose/latest/history/DST/monitoring checks:
  **151 passed**, plus **5 subtests**. The separately enabled historical
  migration-014 test passed as well: **152 distinct passed tests** total.
- Real `psql -v ON_ERROR_STOP=1 -f init.sql` initialization passed in a fresh
  local PostgreSQL 15 database, including all relative migration includes.
- The new bootstrap test proves that direct inserts and the writer use the same
  sequence and that the sequence survives removal of the temporary compact table.
- Both final images were built for **Linux amd64** from committed source
  `cf58dd02b791dc14903e0e39c7041ffcc00d56f8`.
- The ingestor image wrote eight direct rows and processed an actual callback
  payload, producing **11 compact rows**, one raw message and one counter row.
- The API image ran its real HTTP server against that fixture without selectors:
  `/health` returned database connected; history and latest returned the same
  two buckets with values **[0.0, 286.5]**.
- Both Compose contracts passed again after the final image references changed.
- A single non-blocking TestClient dependency deprecation warning appeared.

## Prepared image references

- `schoolheroz-api:compact-only-cf58dd02b791`
  - Local image ID: `sha256:00cc162704a95208d678b2f5316eb9209fa359e60fc1d1f7c46fb4f9cc921368`.
- `schoolheroz-shelly-ingestor:compact-only-cf58dd02b791`
  - Local image ID: `sha256:a952c0c30e6766f4b7d69013f45169bd6fbaec8a6b2d7aea84ae306965839701`.

The production Compose references the source-specific release tags above.
The former image pins required mode selectors and would be incompatible if
recreated without them. Prepared-image identities are recorded separately;
source-specific tags preserve the release reference across Docker image-store
formats when the images are saved/loaded.

This request did not apply a VPS rollout. Load the verified images on the target
before installing the new production Compose and recreating only API/ingestor.
Do not deploy the selector-free Compose with the preceding images, and do not
execute `init.sql` against the populated production database. The live compact
schema already exists and no production DB migration is required here.

Unrelated dirty files in the main checkout remained byte-for-byte unchanged.
The code and release preparation are on `codex/shelly-sequence-detach`.

Machine-readable receipt: [COMPACT-ONLY-SOURCE-20261005.json](COMPACT-ONLY-SOURCE-20261005.json).
