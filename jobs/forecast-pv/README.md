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

The VPS uses `upat-forecast-pv.service` / `.timer` daily at 23:00
Europe/Athens, after the 22:50 weather refresh. The legacy PV cron was removed
on 2026-09-29. The release overlay pins the image
through `/etc/upat-nzc/forecast-pv.env` (`FORECAST_PV_IMAGE=sha256:...`).
Use `systemctl start upat-forecast-pv.service` for manual production runs and
`journalctl -u upat-forecast-pv.service` for logs.
`verify.sql` checks a saved run using the `run_id` and invocation `since` values.

Deployed source: `0eb7114649f68353bf6486dacf7e16f992e236c7`.
Image: `sha256:5c0338d91a07c9603ddd17f312dfb4487aa087470aa69f718022974bf83c9040`.
The manual service run saved run `141` for 2026-09-30: 24 distinct hours and
289.926523378398 kWh, with stored weather inputs and energy totals verified.
The first scheduled run is pending. Backups, verification receipts and the
guarded rollback script are in `/opt/upat-forecast-pv-release-0eb7114/` on the VPS.
