# VPS operational configuration

This directory records the log-retention configuration used by the production
VPS. The files are deployment inputs; system logs, logrotate state, backups,
and journal data do not belong in the repository.

It also records the isolated systemd schedule for the production PV ingestor.
The tracked files never contain live FusionSolar or PostgreSQL credentials.

## Policy

- The system journal is rotated and vacuumed daily at `04:30 Europe/Athens`.
  Entries older than 14 days are removed, and retained journal files are capped
  at 500 MB.
- The seven application and maintenance logs under `/var/log` are checked
  daily, retained for at most 14 rotations/14 days, rotated early at 50 MB per
  file, and compressed after one rotation.
- `/var/log/btmp` is checked daily by the standard logrotate timer, rotated
  weekly or early at 50 MB, and limited to two rotated files/14 days.

The application jobs are short-lived cron commands that reopen their log file
on each run. The rule therefore uses rename plus `create`, with
`delaycompress`, instead of `copytruncate`.

## Install

Run from the repository root on the VPS. Back up the distribution `btmp` rule
before replacing it:

```bash
sudo cp -a \
  /etc/logrotate.d/btmp \
  /root/btmp.logrotate.backup-before-upat-retention

sudo install -m 0644 \
  ops/logrotate/upat-nzc-jobs \
  /etc/logrotate.d/upat-nzc-jobs
sudo install -m 0644 \
  ops/logrotate/btmp \
  /etc/logrotate.d/btmp

sudo logrotate --debug /etc/logrotate.conf
```

Install and enable the journal-retention timer:

```bash
sudo install -m 0644 \
  ops/systemd/upat-journal-vacuum.service \
  /etc/systemd/system/upat-journal-vacuum.service
sudo install -m 0644 \
  ops/systemd/upat-journal-vacuum.timer \
  /etc/systemd/system/upat-journal-vacuum.timer

sudo systemd-analyze verify \
  /etc/systemd/system/upat-journal-vacuum.service \
  /etc/systemd/system/upat-journal-vacuum.timer
sudo systemctl daemon-reload
sudo systemctl enable --now upat-journal-vacuum.timer
```

## Verify

```bash
sudo logrotate --verbose /etc/logrotate.conf
systemctl is-enabled upat-journal-vacuum.timer
systemctl is-active upat-journal-vacuum.timer
systemctl list-timers upat-journal-vacuum.timer --no-pager
journalctl --disk-usage
```

The first normal logrotate run may rotate an existing `btmp` file immediately
when it already exceeds 50 MB. This preserves the records in `btmp.1`; it does
not selectively remove records by event timestamp.

## Roll back the configuration

Disable the custom journal timer before removing its units:

```bash
sudo systemctl disable --now upat-journal-vacuum.timer
sudo rm -f \
  /etc/systemd/system/upat-journal-vacuum.service \
  /etc/systemd/system/upat-journal-vacuum.timer
sudo systemctl daemon-reload

sudo rm -f /etc/logrotate.d/upat-nzc-jobs
sudo cp -a \
  /root/btmp.logrotate.backup-before-upat-retention \
  /etc/logrotate.d/btmp
sudo logrotate --debug /etc/logrotate.conf
```

Rolling back these configuration files does not restore journal entries or
rotated logs that have already expired.

## PV ingestor schedule

The production PV collector is a systemd oneshot service, not a long-running
container. Its timer runs daily at `01:15 Europe/Athens`. Every run requests the
latest three completed Athens-local dates, so provider-delayed or previously
partial rows are healed by the same idempotent PostgreSQL upserts. The service
uses the stable `upat-pv` site key and records `scheduled` provenance.

The unit pins the exact image digest validated by the controlled production run
of code commit `a1aa0b8`. It has no automatic service restart, so one timer
event produces at most one bounded FusionSolar attempt. A 20-minute timeout
prevents a hung job from remaining attached indefinitely. PostgreSQL is not
restarted or reconfigured by this service.

Runtime isolation includes a read-only container filesystem, an unprivileged
container user, no Linux capabilities, no-new-privileges, bounded memory/CPU/
PIDs, no host mounts, and access only to the existing internal Compose network.
The persistence adapter still commits the whole batch in one transaction,
serializes writes per site with an advisory lock, and uses idempotent primary
keys for the rolling window.

### Preconditions

Before installing or enabling the timer, verify all of the following:

- the legacy `mountzou/upat-pv-cron` GitHub workflow is disabled;
- migration `010_pv_actual_telemetry.sql` is already applied;
- PostgreSQL reports healthy;
- Docker network `upat-nzc-mqtt-db_default` exists;
- image digest
  `sha256:8e3d500ec004a9cfb3114475d80f47aac8647e332de944bd0ac122495dfec6b9`
  exists locally on the VPS;
- no other PV scheduler is installed or active.

### Prepare the root-only environment

Create the credential file only once. If the destination already exists, do
not copy the example over it because that would overwrite its values:

```bash
sudo install -d -m 0700 /etc/upat-nzc
sudo cp --no-clobber \
  ops/systemd/upat-pv-ingestor.env.example \
  /etc/upat-nzc/pv-ingestor.env
sudo chown root:root /etc/upat-nzc/pv-ingestor.env
sudo chmod 0600 /etc/upat-nzc/pv-ingestor.env
sudoedit /etc/upat-nzc/pv-ingestor.env
sudo stat -c '%a %U:%G %n' /etc/upat-nzc/pv-ingestor.env
```

`cp --no-clobber` leaves an existing credential file unchanged. The containing
directory is already mode `0700`, so the newly copied template is not exposed
before its file mode is tightened to `0600`.

Fill every value in the file. The PostgreSQL settings must use the internal
Compose endpoint (`postgres:5432`). Do not paste the populated file into logs,
chat, commits, or command-line arguments.

### Install and verify without running ingestion

Run from a clean checkout containing this ops batch:

```bash
sudo install -m 0644 \
  ops/systemd/upat-pv-ingestor.service \
  /etc/systemd/system/upat-pv-ingestor.service
sudo install -m 0644 \
  ops/systemd/upat-pv-ingestor.timer \
  /etc/systemd/system/upat-pv-ingestor.timer

sudo systemd-analyze verify \
  /etc/systemd/system/upat-pv-ingestor.service \
  /etc/systemd/system/upat-pv-ingestor.timer
sudo systemd-analyze calendar '*-*-* 01:15:00 Europe/Athens'
sudo systemctl daemon-reload
sudo systemctl show upat-pv-ingestor.service \
  -p Type -p Restart -p TimeoutStartUSec -p EnvironmentFiles
```

These commands validate and load the units but do not call FusionSolar and do
not write to PostgreSQL.

### Enable and monitor

Enable the timer only after the preconditions and credential-file permissions
have been checked. Starting the timer is not the same as manually starting the
service:

```bash
sudo systemctl enable --now upat-pv-ingestor.timer
systemctl is-enabled upat-pv-ingestor.timer
systemctl is-active upat-pv-ingestor.timer
systemctl list-timers upat-pv-ingestor.timer --no-pager
systemctl is-active upat-pv-ingestor.service
```

After the first scheduled run, inspect its result and journal before querying
the persisted rows read-only:

```bash
systemctl show upat-pv-ingestor.service \
  -p Result -p ExecMainStatus -p ExecMainStartTimestamp -p ExecMainExitTimestamp
journalctl -u upat-pv-ingestor.service --since today --no-pager
```

### Roll back scheduling

Disabling the timer stops future collections. It does not alter PostgreSQL,
remove already persisted rows, restart the database, or re-enable the GitHub
workflow:

```bash
sudo systemctl disable --now upat-pv-ingestor.timer
sudo rm -f \
  /etc/systemd/system/upat-pv-ingestor.service \
  /etc/systemd/system/upat-pv-ingestor.timer
sudo systemctl daemon-reload
```

Keep `/etc/upat-nzc/pv-ingestor.env` during a temporary rollback. Delete it only
as a separate, explicitly approved credential-retirement action.
