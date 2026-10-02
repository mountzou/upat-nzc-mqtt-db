# Weather forecast

Fetches eight days of hourly Open-Meteo forecasts and upserts them into
`weather_hourly_forecasts`. See the [root README](../../README.md#weather-forecast-job)
for configuration.

From the repository root:

```sh
# Build
docker compose --profile jobs build forecast-weather

# Run locally: calls Open-Meteo and writes to the configured database
docker compose --profile jobs run --rm forecast-weather

# Offline tests (requires requirements.txt dependencies)
python -m unittest discover -s jobs/forecast-weather -p 'test_*.py'
```

VPS: `upat-forecast-weather.timer` runs daily at `22:50 Europe/Athens`.
The legacy cron was removed on 2026-09-29. The manual run and first scheduled
run that day at 22:50 were verified: 192 refreshed hours for 2026-09-29 through
2026-10-06. `verify.sql` checks the stored coverage and collection times.
Manual VPS runs use `systemctl start upat-forecast-weather.service`.
The release image is pinned through `/etc/upat-nzc/forecast-weather.env`.
Release receipts and rollback backups remain in
`/opt/upat-forecast-weather-release-9920730/` on the VPS.
