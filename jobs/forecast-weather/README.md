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

VPS migration is pending: the legacy cron calls `weather-collector`, which is
absent from the updated Compose files. Replace it with `upat-forecast-weather`
as part of the [scheduler cutover](CUTOVER.md), preserving
`22:50 Europe/Athens`.
