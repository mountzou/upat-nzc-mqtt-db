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

The PV collector is a systemd oneshot service with a daily timer at
`01:15 Europe/Athens`. The service template uses the shared
`ops/pv-ingestor-run.sh` launcher, a separately validated image digest, and a
persistent API ledger for scheduled and manual/backfill requests. It retains
the three completed Athens dates, `upat-pv` site key, scheduled provenance,
20-minute timeout, and no automatic restart.

The initial shared-control installation was completed on 2026-09-05. Its
historical receipt and activation/rollback procedure are documented in
[PV_API_CONTROL.md](PV_API_CONTROL.md). Preserve the existing ledger during
upgrades; do not repeat initialization. Future changes follow commit, build,
then separately authorized rollout. Installing only the unit or reusing the
old image will not provide the new protections. No PostgreSQL migration or
database restart is needed for these controls.

## Production Compose consolidation

See [PRODUCTION_COMPOSE.md](PRODUCTION_COMPOSE.md) for the verified replacement
of the historical deployment overlays, the read-only equivalence checker, and
the verified API-only activation and rollback boundary. Other services were
not restarted.

## Scheduled energy aggregation

[ENERGY_AGGREGATOR.md](ENERGY_AGGREGATOR.md) records the stable cron launcher,
pinned production image, fixed counter cutover and installation rollback.
The old migration wrapper is no longer an active scheduler input.
