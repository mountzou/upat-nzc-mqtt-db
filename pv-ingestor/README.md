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

Every live request now requires an initialized, persistent `PV_API_STATE_DIR`.
The account ledger records the attempt **before** HTTP, including failed and
interrupted attempts. All three endpoints are covered; requests are never
retried or redirected automatically. History calls are separated by at least
65 seconds across device types and runs. A whole-run file lock serializes API
access across containers that share the directory.

The initial local policy allows at most 12 history attempts in a rolling 24h,
with the final two available only to scheduled runs. This is a conservative
operator policy, not a verified Huawei account quota. Full-run preflight checks
the planned one/two history calls before login. The ledger also caps login at
5 attempts per rolling 10 minutes and device discovery at 12 per rolling 24h.

Account `failCode=407` sets a shared 24h cooldown; `429`/HTTP 429 sets 15 minutes;
other failed responses or transport errors set 15 minutes. A longer
`Retry-After` wins. Expiry permits a future requested run; it never starts an
automatic retry and does not prove that Huawei has unblocked the account.
Unknown outcomes after interruption and initial activation require a 24h hold.

The durable ledger is a separate SQLite file, outside production PostgreSQL.
Missing/corrupt state, a different account/policy, or an active run prevents API
calls. JSON events contain endpoint, attempt/run IDs, trigger, device type/count,
time window, status, numeric fail code, timing, and outcome. They omit account
names, device IDs, credentials, tokens, provider messages, and raw payloads.

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

After the separately reviewed VPS activation in
[`ops/PV_API_CONTROL.md`](../ops/PV_API_CONTROL.md), use the same launcher and
ledger as the scheduled job:

```bash
sudo /usr/local/sbin/upat-pv-ingestor manual --no-save-to-db
```

This consumes real API quota even though it does not persist telemetry.
The old Compose preview has no shared state mount and therefore fails before
HTTP with the new code. Do not create a separate local ledger for the production
account to bypass a refusal. Use fixture mode for development.

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
procedure is documented in `ops/PV_API_CONTROL.md`. Do not enable that timer while any
legacy FusionSolar scheduler is active.
