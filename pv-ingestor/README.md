# PV ingestor

This service performs a bounded FusionSolar fetch, validates the response, keeps
per-device provenance, derives plant-level readings, and emits either a concise
summary or the complete `pv-ingestion-batch-v1` JSON document to stdout.
PostgreSQL persistence exists as a separate, explicit opt-in step and is off by
default.

Live and fixture executions carry distinct provenance in `source_kind` and in
the deterministic run key; fixture validation can therefore never masquerade
as a live collection.

The default preview deliberately has:

- no PostgreSQL writes;
- no scheduled execution or production Compose entry;
- no automatic retries;
- no `/stations` discovery call (the plant code is explicit);
- no raw credential, token, serial-number, coordinate, or response logging;
- no assumption that a healthy day contains 288 readings.

## Call contract

One live run uses sequential calls only:

1. one login;
2. one device-list request;
3. one historical request for all inverters together;
4. one separate historical request for the grid meter, when present.

The default window is the latest three completed `Europe/Athens` dates. The
FusionSolar application response and `failCode` are checked even when HTTP is
successful. A `407`/rate-limit response fails immediately without a retry storm.

## Offline validation

No network or database is used:

```bash
cd pv-ingestor
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -p 'test_*.py'
python main.py --fixture tests/fixtures/fusionsolar_sample.json \
  --target-date 2026-08-24 --lookback-days 1
```

Use `--emit-json` to inspect the entire persistence-ready batch on stdout.
Fixture mode is permanently read-only and rejects `--save-to-db`.

## Explicit live preview

Set the following only in a local/private environment; never commit their
values:

```text
FUSIONSOLAR_BASE_URL=https://.../thirdData
FUSIONSOLAR_USERNAME=...
FUSIONSOLAR_SYSTEM_CODE=...
FUSIONSOLAR_PLANT_CODE=NE=...
FUSIONSOLAR_LOOKBACK_DAYS=3
FUSIONSOLAR_INCLUDE_METER=true
```

Then run the isolated preview profile:

```bash
docker compose --profile pv-ingestor-preview run --rm --no-deps pv-ingestor
```

The command makes live read calls, but the resulting batch is not written to a
file, Firestore, or PostgreSQL.

## Explicit local PostgreSQL persistence

Persistence requires all of the following:

- a live run (`--fixture` is rejected);
- `--save-to-db` or `PV_INGESTOR_SAVE_TO_DB=true`;
- a stable `--site-key` or `PV_SITE_KEY`;
- the standard `POSTGRES_HOST`, `POSTGRES_INTERNAL_PORT`, `POSTGRES_DB`,
  `POSTGRES_USER`, and `POSTGRES_PASSWORD` variables;
- migration `010_pv_actual_telemetry.sql` already applied.

One batch is committed in one transaction. Plant and device metadata are
upserted, each execution gets a distinct audit run, and the device/plant
five-minute primary keys make rolling-window re-fetches idempotent. Any failed
statement rolls back the whole batch. The existing preview Compose profile does
not enable persistence or receive database credentials.

## Production schedule

Production scheduling is deliberately separate from the persistence-free
Compose preview. The reviewed systemd service and timer live under
`ops/systemd/`; their install, credential, validation, monitoring, and rollback
procedure is documented in `ops/README.md`. Do not enable that timer while any
legacy FusionSolar scheduler is active.
