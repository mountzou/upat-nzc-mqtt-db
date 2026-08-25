# VPS retention rules

This directory records the log-retention configuration used by the production
VPS. The files are deployment inputs; system logs, logrotate state, backups,
and journal data do not belong in the repository.

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
