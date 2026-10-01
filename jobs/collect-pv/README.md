# PV collection

This job performs a bounded FusionSolar fetch, validates the response, keeps
per-device provenance, derives plant-level readings, and emits a concise JSON
summary to stdout.
Every run stores the collected telemetry in PostgreSQL.

The job has:

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

The meter is collected whenever present.

Every run fetches the latest three completed `Europe/Athens` dates. There is
no separate backfill mode. The FusionSolar application response and `failCode`
are checked even when HTTP is successful. A `407`/rate-limit response fails
immediately without a retry storm.

Every live request now requires an initialized, persistent `PV_API_STATE_DIR`.
The account ledger records the attempt **before** HTTP, including failed and
interrupted attempts. All three endpoints are covered; requests are never
retried or redirected automatically. History calls are separated by at least
65 seconds across device types and runs. A whole-run file lock serializes API
access across containers that share the directory.

The local policy allows at most 12 history attempts in a rolling 24h,
shared by all executions. This is a conservative
operator policy, not a verified Huawei account quota. Full-run preflight checks
capacity for both history calls before login. The ledger also caps login at
5 attempts per rolling 10 minutes and device discovery at 12 per rolling 24h.

Account `failCode=407` sets a shared 24h cooldown; `429`/HTTP 429 sets 15 minutes;
other failed responses or transport errors set 15 minutes. A longer
`Retry-After` wins. Expiry permits a future requested run; it never starts an
automatic retry and does not prove that Huawei has unblocked the account.
Unknown outcomes after interruption and initial activation require a 24h hold.

The durable ledger is a separate SQLite file, outside production PostgreSQL.
Missing/corrupt state, a different account/policy, or an active run prevents API
calls. JSON events contain endpoint, attempt/run IDs, device type/count,
time window, status, numeric fail code, timing, and outcome. They omit account
names, device IDs, credentials, tokens, provider messages, and raw payloads.

## Offline validation

No network or database is used:

```bash
cd jobs/collect-pv
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -p 'test_*.py'
```

## PostgreSQL persistence

Persistence requires all of the following:

- the standard `POSTGRES_HOST`, `POSTGRES_INTERNAL_PORT`, `POSTGRES_DB`,
  `POSTGRES_USER`, and `POSTGRES_PASSWORD` variables;
- migrations `010_pv_actual_telemetry.sql` and `018`–`021` applied.

One batch is committed in one transaction. Plant and device metadata are
upserted, each execution gets a distinct audit run, and the device/plant
five-minute primary keys make rolling-window re-fetches idempotent. Any failed
statement rolls back the whole batch.

## Production schedule

The local source migration to `jobs/collect-pv` is complete. The launcher
`run.sh`, `upat-collect-pv.service`, `upat-collect-pv.timer` and private env
example are versioned here. The prepared timer retains daily
`01:15 Europe/Athens`, `Persistent=true` and the existing runtime limits.

The VPS still uses `upat-pv-ingestor.service` / `.timer` and
`/usr/local/sbin/upat-pv-ingestor`. Production cutover is pending. The new
launcher retains `/etc/upat-nzc/pv-ingestor.env` and `PV_INGESTOR_IMAGE`
for configuration compatibility. Migrations `018`–`021` remove
`trigger_kind`, `site_key`, `source_kind` and `code_version` respectively.

After cutover, trigger the same job manually with
`sudo systemctl start upat-collect-pv.service`. Both the timer and manual start
use `upat-collect-pv run`, which persists to PostgreSQL. Missing data within
the three-date window can be recovered by the next successful run.

The launcher uses the existing account-specific ledger. Preserve that directory,
its request history and cooldowns; never initialize a replacement. The existing
ledger is compatible.
Update the EnergyPlus PV adapter first, then the API readers. Stop the old
scheduler and wait for active runs before applying migrations `018`–`021`
and enabling the new timer. See [the API control policy](../../ops/PV_API_CONTROL.md)
for validation, monitoring and rollback boundaries.
