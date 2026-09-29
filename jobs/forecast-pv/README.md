# PV forecast

Predicts next-day hourly PV production using weather stored by `forecast-weather`
and the tracked Random Forest model. See the [root README](../../README.md#pv-forecast-job)
for model details and configuration. Feature names and their order are read
from `model.pkl` itself (`feature_names_in_`); no separate feature file is needed.
Each saved run records the `model_version` from `model_manifest.json` in its metadata.

Requires a populated `weather_hourly_forecasts` table (migrations 004 and 013).
The query assumes one weather location in the table. Coordinates from the env
are recorded as PV run metadata. The job reads tomorrow in the configured weather
timezone, requires all 24 stored
local hourly slots and finite values, and rejects weather older than
24 hours (`WEATHER_MAX_AGE_HOURS` in the code). Wind speed is converted from m/s to km/h
with ×3.6. It makes no Open-Meteo requests, including in preview mode.

Output is a daily summary; add `--verbose` to also display hourly predictions.

From the repository root:

```sh
# Build
docker compose --profile jobs build forecast-pv

# Preview: reads PostgreSQL without saving PV predictions
docker compose --profile jobs run --rm forecast-pv --no-save-to-db

# Normal run: writes to the configured database by default
docker compose --profile jobs run --rm forecast-pv

# Offline tests (requires requirements.txt dependencies)
python -m unittest discover -s jobs/forecast-pv -p 'test_*.py'
```

VPS migration is pending. The legacy cron calls `pv-prediction`; switching to
`upat-forecast-pv` requires a separate deployment and scheduler cutover.
