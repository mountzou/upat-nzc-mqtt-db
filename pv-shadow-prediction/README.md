# Local PV shadow lane

This service is intentionally separate from the operational PV job. It reads one
successful D+1 champion snapshot, verifies 24 persisted feature rows, and runs an
explicit shadow candidate without calling Open-Meteo or changing the public API.
Older champion rows that omitted `doy_sin`/`doy_cos` from `raw_features` are
supported by reconstructing only those deterministic timestamp-derived values;
the replay parity check still has to match all 24 stored champion predictions.

The champion tables store artifact filenames, not hashes. The freeze therefore
records the observed deployed image and its dirty-checkout limitation explicitly;
the control additionally requires exact 24-hour prediction parity before it can
be saved as a successful replay.

The bundled `champion-replay` candidate is only a Batch 1 pipeline control. It
must reproduce the champion's 24 stored predictions and is not a model challenger.

Apply the additive migration to an existing local database before enabling writes:

```bash
docker compose exec -T postgres sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/migrations/007_pv_shadow_forecasts.sql'
```

Safe preview mode is the default:

```bash
docker compose --profile shadow run --rm --no-deps pv-shadow-prediction
```

After the migration, an explicit local persistence smoke test can use:

```bash
docker compose --profile shadow run --rm pv-shadow-prediction \
  --candidate-id champion-replay --save-to-db
```

There is no production Compose service or cron entry for this lane yet.
