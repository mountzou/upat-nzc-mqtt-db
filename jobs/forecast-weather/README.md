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
python -m unittest discover -s tests -p 'test_weather_schema.py'
```

VPS: `upat-forecast-weather.timer` runs daily at `22:50 Europe/Athens`.
The legacy cron was removed on 2026-09-29; the manual run stored and verified
192 hours. Manual VPS runs use `systemctl start upat-forecast-weather.service`.
The [cutover guide](CUTOVER.md) is retained until the first daily run is verified.
