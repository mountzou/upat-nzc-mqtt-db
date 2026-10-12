# VPS operational configuration

This directory records the log-retention configuration used by the production
VPS. The files are deployment inputs; system logs, logrotate state, backups,
and journal data do not belong in the repository.

It also records the isolated systemd schedule for the production PV collector.
The tracked files never contain live FusionSolar or PostgreSQL credentials.

## Policy

- The system journal is rotated and vacuumed daily at `04:30 Europe/Athens`.
  Entries older than 14 days are removed, and retained journal files are capped
  at 500 MB.
- The four application and maintenance logs under `/var/log` are checked
  daily, retained for at most 14 rotations/14 days, rotated early at 50 MB per
  file, and compressed after one rotation.
- `/var/log/btmp` is checked daily by the standard logrotate timer, rotated
  weekly or early at 50 MB, and limited to two rotated files/14 days.

Jobs writing these file logs reopen them on each run. The rule therefore uses
rename plus `create`, with `delaycompress`, instead of `copytruncate`.
The `forecast-weather`, `forecast-pv` and `aggregate-energy` jobs write to the
systemd journal.

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

## PV collection schedule

The PV collector runs daily at `01:15 Europe/Athens` through
`upat-collect-pv.service` / `.timer`. It fetches the last three completed
Athens dates, with a 20-minute timeout and no automatic restart.

The [collector guide](../jobs/collect-pv/README.md) owns the API policy,
persistent ledger, verification commands and upgrade/recovery requirements.
Preserve the existing ledger and its cooldowns when changing the job.

## Production Compose ownership

See [the current production ownership guide](postgres-volume/COMPOSE-OWNERSHIP.md)
for PostgreSQL's systemd ownership, the canonical Compose configuration and the
read-only `ops/postgres-volume/check-compose.py` validator.

## Scheduled energy aggregation

[ENERGY_AGGREGATOR.md](ENERGY_AGGREGATOR.md) records the hourly systemd timer,
pinned production image, fixed counter cutover and installation rollback.
