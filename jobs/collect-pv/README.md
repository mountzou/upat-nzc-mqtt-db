# PV collection

The collector fetches FusionSolar telemetry, validates it, derives plant readings
and commits it to PostgreSQL. It emits a concise JSON summary. The public
history/forecast response is documented in [PV energy](../../ops/PV_ENERGY.md).

## Collection contract

Every scheduled or manual run fetches the last three completed Europe/Athens
dates. There is no separate backfill mode or configurable lookback. Calls are
sequential: one login, one device-list request, one history request for all
inverters, then a separate grid-meter history request when a meter is present.
No stations-discovery request is made.

HTTP and application success/failCode are both checked. Requests have no automatic
retries or redirects. Missing intervals remain missing; 288 observations are
not assumed for every day. Credentials, tokens, serial numbers, coordinates and
raw responses are not logged.

## API policy

All runs share conservative local limits, not a verified Huawei account quota.
Preflight checks capacity for both history calls before login; scheduled runs
have no separate budget or priority.

| Control | Policy |
| --- | --- |
| History attempts | 12 per rolling 24h, across runs and device types |
| History pacing | At least 65 seconds between request starts, including across runs |
| Login attempts | 5 per rolling 10 minutes |
| Device-list attempts | 12 per rolling 24h |
| Account limit (`failCode=407`) | Shared cooldown of at least 24h |
| System throttling (`429`/HTTP 429) | Shared cooldown of at least 15 minutes |
| Other unsuccessful/uncertain responses | Shared cooldown of at least 15 minutes |
| `Retry-After` | A longer server delay takes precedence |
| Initial setup or interrupted pending attempt | 24h hold |

Cooldown expiry permits a later requested run; it neither retries automatically
nor proves the provider has unblocked the account. There are no probe calls.
Other clients using the same account must share this launcher/ledger or be
disabled; their requests cannot otherwise be accounted for.

## Persistent ledger

Every live call requires the initialized `PV_API_STATE_DIR`. The account-scoped
`api-control.sqlite3` stores policy/version, an account hash, shared cooldown
and request attempts. SQLite synchronous FULL commits each attempt before HTTP;
failed and interrupted attempts consume budget too. An unfinished attempt becomes
`unknown_after_interruption` on the next run and imposes one conservative 24h hold.

Local refusals create no provider-attempt row and do not extend an existing
cooldown. Missing/corrupt state, account/policy mismatch or another active run
blocks calls. A file lock serializes the API section across runs; PostgreSQL's
separate advisory lock protects persistence. The obsolete scheduled-reserve
metadata is ignored when reading an existing ledger.

Started/finished events go to stderr and the service journal. Ledger records
retain endpoint, attempt/run IDs, device type/count, bounds, timing and numeric
outcome codes, without credentials, account names, device IDs, provider messages
or payloads. Manual runs retain ledger evidence even without journal capture.

Preserve the existing ledger, request history and cooldowns through upgrades
and recovery. It is not recreated during runs, reset at midnight or deleted
with the temporary container. Initialization is only for first setup and refuses
an existing ledger; production upgrades must not initialize a replacement.

## PostgreSQL persistence

Requires the standard `POSTGRES_HOST`, `POSTGRES_INTERNAL_PORT`, `POSTGRES_DB`,
`POSTGRES_USER`, `POSTGRES_PASSWORD` settings and migrations
`010_pv_actual_telemetry.sql` and `018`–`021`.

Each batch commits in one transaction, with plant/device metadata upserts,
a distinct ingestion run and idempotent five-minute reading keys. Any failed
statement rolls back the batch. The current schema has removed `trigger_kind`,
`site_key`, `source_kind` and `code_version`; older collectors requiring those
columns are incompatible.

## Schedule and operation

The VPS uses `upat-collect-pv.service` / `.timer`, adopted on 2026-10-01.
The oneshot runs daily at 01:15 Europe/Athens, with `Persistent=true`, a 20-minute
timeout and no automatic restart. Manual starts use the same collection path
and persist telemetry; a subsequent successful run can recover gaps inside
the three-date window.

The launcher `/usr/local/sbin/upat-collect-pv` reads the root-owned mode-0600
`/etc/upat-nzc/pv-ingestor.env` in Docker env-file syntax, without shell-sourcing
credentials. `PV_INGESTOR_IMAGE` must select a locally validated immutable digest,
with `--pull=never`. The existing host ledger is mounted at `/state`.

```sh
# Local ledger status: no network or telemetry writes; state mounted read-only
sudo /usr/local/sbin/upat-collect-pv status

# Observe scheduled execution
systemctl status upat-collect-pv.timer upat-collect-pv.service --no-pager
journalctl -u upat-collect-pv.service -n 50 --no-pager

# Intentional collection: consumes provider budget and writes PostgreSQL
sudo systemctl start upat-collect-pv.service
```

Successful execution requires service exit status, matched ledger attempts/results
and persisted ingestion-run/readings evidence. Timer activity or HTTP success
alone is insufficient. On throttling, inspect the recorded outcome/cooldown and
wait for a permitted scheduled/requested run.

## Upgrade and recovery

Validate and commit scoped source before building the candidate; record the
commit/image mapping and verify current schema/ledger compatibility. Pause only
the PV timer and wait for an active collection to finish before changing its
image/configuration. Preserve the private env file and existing ledger.

Verify the next normal execution through its journal, request pacing and stored
data. If acceptance fails, pause the timer and retain the ledger and PostgreSQL
data while selecting a verified compatible image/configuration pair. Restoring
an old collector must not bypass API accounting or depend on removed columns.
Installing units alone does not install these controls.

## Offline validation

Use a Python environment with `requirements.txt` installed. Offline tests use
mocked HTTP and skip the opt-in database suite:

```sh
cd jobs/collect-pv
PV_INGESTOR_INTEGRATION_TEST=0 PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -p 'test_*.py'
```
