# Production cutover

Prepared for `/opt/upat-nzc-mqtt-db`; activation is pending. Keep the VPS's
existing `.env`, `docker-compose.yml` symlink and unrelated deployment changes.
Do not use `git pull`, reset, or an untargeted Compose command on that checkout.

## Prepare a release

1. Commit and review these files before building. Export the committed
   `jobs/forecast-weather/` directory to a separate release directory on the VPS
   and record its full commit and file hashes. Do not build from the dirty VPS
   checkout.
2. Run `python3 /opt/upat-nzc-mqtt-db/ops/postgres-volume/check-compose.py --check-default`.
   Recheck the single weather cron and absence of a running collector. Back up
   the current crontab, production Compose, and any existing target job/unit/env
   files into a private release backup. Record container IDs and restart counts.
3. Build from the release directory on the VPS, labeling the image with the
   source commit. Record `docker image inspect --format '{{.Id}}' IMAGE`.
   Run the collector tests with `--network none`, an overridden Python entrypoint
   and the test file mounted read-only. The image excludes tests by design.
4. Prepare a copy of the installed production Compose, changing only the weather
   service key to `forecast-weather`, its build context to
   `./jobs/forecast-weather`, and its container name to `upat-forecast-weather`.
   Compare resolved models privately: every other service, network, environment,
   volume and profile must match. Never print resolved credentials.
5. Stage `forecast-weather.env` containing only
   `FORECAST_WEATHER_IMAGE=sha256:<verified-image-id>`. The release overlay removes
   `build` and the service uses `--pull never`, so scheduled runs cannot rebuild
   or fetch a different image. Verify the staged units with `systemd-analyze verify`.

## Activate the reviewed release

Perform the cutover outside the 22:50 dispatch minute. Recheck the saved file and
crontab hashes immediately before changes; stop if they changed.

1. Acquire `/var/lock/weather-collector.lock` and hold it through installation.
   Remove exactly the previously reviewed weather cron line; preserve every
   other line. Confirm no weather collector is running.
2. Install only the committed job directory and the reviewed Compose candidate.
   Preserve the old `weather-collector/` source and image for rollback.
   Install `forecast-weather.env` at `/etc/upat-nzc/forecast-weather.env` with
   mode `0600`, and the two units under `/etc/systemd/system/` with mode `0644`.
3. Run `systemctl daemon-reload`, repeat the production validator, and confirm
   unrelated containers have the same IDs/restart counts. Release the lock.
4. Run `systemctl start upat-forecast-weather.service` once, then inspect
   `Result`, `ExecMainStatus`, `ExecMainStartTimestamp` and its journal. This step
   calls Open-Meteo and upserts forecasts. Do not retry blindly after a failure.
5. Verify persisted rows using the command below, with `SINCE` set to that
   invocation's start time and `START_DATE` to its Athens-local date.
6. Only after successful verification, run
   `systemctl enable --now upat-forecast-weather.timer`. Confirm the next trigger
   is 22:50 Europe/Athens and the old cron is absent. Verify the first scheduled
   execution's service result and database rows too.

```sh
docker exec -i iot_postgres sh -c \
  'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"' \
  sh -v start_date="$START_DATE" -v since="$SINCE" -v forecast_days=8 \
  -v latitude=37.068 -v longitude=22.026 -v timezone=Europe/Athens \
  < /opt/upat-nzc-mqtt-db/jobs/forecast-weather/verify.sql
```

Use the reviewed job configuration if it differs from these audited defaults.
The read-only query must report 192/192/192 expected/stored/refreshed hours for
this eight-day window. It checks the existing local-clock storage contract;
handling 23/25-hour DST days is outside this scheduler migration.

## Rollback

Disable and stop the new timer and service; its cleanup targets only the current
invocation's container. Acquire the legacy lock, then restore the backed-up
Compose and previous target files. Restore the saved crontab only if all other
entries are unchanged; otherwise reinsert only the removed weather line.
Reload systemd and verify the old schedule and unrelated services before
releasing the lock. Keep the release image and backups for diagnosis. Forecast
rows already upserted remain in place; rollback does not delete database data.
