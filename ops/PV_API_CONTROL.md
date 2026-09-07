# PV API accounting and shared request control

Historical installation record: deployed on the VPS on 2026-09-05 after explicit approval. The exact
image is `upat-nzc-mqtt-db-pv-ingestor@sha256:76b2d48b42d0a84ad88ba2cd099cb21239eaca1977b3dfb7df5b7d3c37750eb6`.
At that deployment check the timer was enabled and active; initialization and
status verification made zero API attempts. The initial hold was set through
**2026-09-06 21:18:05 Europe/Athens**, making 2026-09-07 01:15 the first eligible
scheduled execution under the local policy. This installation record does not
establish current provider availability or a successful subsequent ingestion.
No manual live validation was performed during installation.

Source fingerprint: `17678abbfdf64d370524565af0b9ed0fa26f3d9372329d40433fe301edf21e78`.
Private prior settings and installation evidence are retained on the VPS under
`/root/pv-api-control-deployment-20260905T181245Z`. Local evidence is under
`backups/pv-api-control-deployment-20260905T181245Z/`.

This deployment required **no PostgreSQL schema change, migration, restart,
restore, or volume operation**. Postchecks confirmed the same healthy database
container, start time, restart count, data volume, last PV run and observation.
Existing ingestion transactions and telemetry upserts are unchanged.

## Policy and boundaries

| Control | Initial local policy |
| --- | --- |
| History budget | 12 attempted requests per rolling 24 hours, across all device types |
| Scheduled reserve | Manual/backfill requests cannot increase usage beyond 10; scheduled runs may reach 12 |
| History pacing | At least 65 seconds between request start times, including across runs |
| Login | At most 5 attempts per rolling 10 minutes |
| Device discovery | At most 12 attempts per rolling 24 hours |
| Account limit (`failCode=407`) | Shared cooldown of at least 24 hours |
| System throttling (`429`, including HTTP 429) | Shared cooldown of at least 15 minutes |
| Other unsuccessful/uncertain response | Shared cooldown of at least 15 minutes |
| `Retry-After` | Longer server delay takes precedence |
| First initialization / interrupted unfinished attempt | 24-hour hold because prior outcome/usage is unknown |
| Retries / redirects | None |

These are deliberately conservative client policies, **not a claim about the
effective Huawei quota**. In particular, 12 is chosen below the user's empirical
warning threshold near 26 history calls. An account may remain blocked after a
cooldown expires. No endpoint is called to probe whether the account is blocked.
The next permitted call happens only in a requested or scheduled run.

At the normal two history calls per run, a daily scheduled run leaves room for
four additional full manual/backfill runs inside that rolling window. This is
a maximum, not a target. A one-day request costs the same history-call budget
as a three-day request. `--skip-meter` uses one history call, when explicitly
appropriate, and preflight reserves only that one.

All updated live entry points require the account ledger. The launcher fixes
its host path to `/var/lib/upat-nzc/pv-api`, binds it to `/state`, and supplies
`PV_API_STATE_DIR=/state`. It uses a root-owned private Docker env file without
shell-sourcing credentials, a digest-pinned image with `--pull=never`, and the
existing runtime limits. Scheduled containers keep their original fixed name;
manual/backfill containers have different names, so scheduled cleanup cannot
remove a manual job. A file lock prevents simultaneous API sections across
processes. This lock is separate from the unchanged PostgreSQL advisory lock.

An old image, a different ledger, or an external client cannot be accounted for
by this local mechanism. All callers using the same Huawei account must use
this launcher/ledger or be disabled. `scheduled` is a trusted operational mode,
not an authorization boundary against a host administrator.

## Durable evidence

`api-control.sqlite3` contains an account hash, policy/version, shared cooldown,
and one row per attempted request. The row is committed with SQLite synchronous
FULL before HTTP. Successful and failed responses update that row. A killed
process leaves a pending row; the next run records its outcome as
`unknown_after_interruption` and establishes one conservative 24h hold. It does
not assume the request was never sent. Locally refused calls are JSON events,
not additional provider attempts, and do not extend the existing cooldown.

Started/finished JSON events go to stderr, hence to the scheduled journal.
Manual stderr is visible in the calling terminal; the SQLite attempt remains
durable even without journal capture. Outcomes distinguish account quota,
system throttling, transport error, non-JSON, HTTP/application error, redirect
refusal, and missing login token. No raw responses, credentials, usernames,
device IDs, or provider-supplied messages are stored in this ledger.

State is not auto-created during live runs, auto-reset at midnight, or deleted
when the temporary container exits. No reset/purge operation is provided.
Policy/account mismatch or inaccessible/corrupt state fails closed. Preserve
the ledger directory through image upgrades and rollbacks. This small API
ledger is not a telemetry backup; retain the existing PostgreSQL backups.

## Reviewed activation procedure

The initial installation described above is complete. These instructions are
retained for reference; do not repeat initialization or reset the existing
ledger. Each future production change requires its own scoped authorization.

1. Verify the scoped change and offline tests, commit the tested source, then
   build/validate a candidate from that commit. Record its real image digest
   and exact Git revision before rollout. The old `a1aa0b8` image
   does not contain the control and must not be used with this launcher.
2. With deployment approval, pause only the PV timer and confirm that no PV
   collection is active. Do not interrupt a running ingestion transaction.
   Confirm that legacy workflows and other clients are inactive. Preserve the
   existing service, environment file, image reference, and timer settings.
3. Review `/etc/upat-nzc/pv-ingestor.env` privately. The launcher uses Docker
   env-file syntax: unquoted literal `KEY=value`, no shell expansion. Existing
   systemd quoting must be adapted without changing credential values. Add the
   verified `PV_INGESTOR_IMAGE=repository@sha256:...` and
   `PV_INGESTOR_CODE_VERSION=...` (the exact Git revision; the launcher also
   accepts `sha256:<source fingerprint>` for historical release records).
   Keep root ownership and mode 0600; do not copy
   the blank example over the existing credentials.
4. Create only the dedicated API state directory, owned by UID/GID 65534 with
   mode 0700. Install the launcher and reviewed service template. Example
   installation commands, for the approved maintenance window:

   ```bash
   sudo install -d -o 65534 -g 65534 -m 0700 /var/lib/upat-nzc/pv-api
   sudo install -m 0755 ops/pv-ingestor-run.sh /usr/local/sbin/upat-pv-ingestor
   sudo install -m 0644 ops/systemd/upat-pv-ingestor.service /etc/systemd/system/upat-pv-ingestor.service
   sudo systemd-analyze verify /etc/systemd/system/upat-pv-ingestor.service /etc/systemd/system/upat-pv-ingestor.timer
   ```

5. Initialize once via `sudo /usr/local/sbin/upat-pv-ingestor initialize`.
   This launches the candidate image with **network disabled**, writes only the
   dedicated ledger, refuses an existing ledger, and starts a 24h hold. It does
   not contact PostgreSQL or Huawei. During an upgrade use the existing ledger;
   do not initialize a replacement to remove a cooldown. Investigate an
   incomplete initialization instead of deleting its state blindly.
6. Inspect `sudo /usr/local/sbin/upat-pv-ingestor status`. This also runs without
   network and mounts state read-only. It reports the local cooldown and recent
   attempt counts; it cannot establish Huawei's current account status. Keep
   all other callers inactive throughout the initial hold. Reload systemd and
   enable only the reviewed PV timer when ready; note that its persistent
   setting may schedule a missed execution. An execution during the hold will
   fail locally without requests.
7. Observe the first permitted scheduled run, rather than adding a test fetch.
   Verify attempt/result pairs, 65s history spacing, service exit, and existing
   ingestion persistence using bounded read-only checks. On a throttle, inspect
   the recorded endpoint, device type, HTTP/failCode and cooldown; do not retry.

After activation, authorized manual commands must use the launcher:

```bash
# Consumes quota; telemetry preview only.
sudo /usr/local/sbin/upat-pv-ingestor manual --no-save-to-db
# Explicitly authorized historical ingestion with normal PostgreSQL upserts.
sudo /usr/local/sbin/upat-pv-ingestor backfill --target-date=2026-09-01 --lookback-days=1 --save-to-db
# Purely local evidence, no network and no telemetry writes.
sudo /usr/local/sbin/upat-pv-ingestor status
```

If activation must be withdrawn, pause the PV timer and retain the ledger and
database. Re-enabling an old collector would bypass these controls; do not
automatically fall back to it. Inspect and approve any subsequent recovery
separately. No database rollback is part of this change.
