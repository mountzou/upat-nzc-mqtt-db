# Production PostgreSQL and Compose ownership

PostgreSQL belongs to `upat-postgres-volume.service`, outside Compose.
`iot_postgres` uses `/mnt/HC_Volume_106884142/pgdata`, restart policy `no`
and no Compose ownership labels. Preserve this ownership and data location.

The canonical configuration is
`/opt/upat-nzc-mqtt-db/docker-compose.prod.yml`. It defines no PostgreSQL
service, postgres_data volume or PostgreSQL depends_on. The
`upat-nzc-mqtt-db_default` network is external; Caddy also uses the external
`nzc-energyplus` network. Select reviewed application image/environment changes
through this configuration.

## Application deployment

The VPS `docker-compose.yml` is a relative symlink to `docker-compose.prod.yml`.
The repository's local-development Compose defines PostgreSQL and must not
replace that symlink.

Before a targeted application change, run the installed read-only validator:

```sh
python3 /opt/upat-nzc-mqtt-db/ops/postgres-volume/check-compose.py --check-default
```

It checks the host guard, database, Compose model, image/environment contract
and container stability without emitting credentials or making changes.
A candidate image/configuration still requires its own validation.

Use the exact canonical project, env file and Compose with a named service and
`--no-deps --no-build --pull never`. An untargeted up/down, remove-orphans or
volume operation can affect database ownership and is not an application update.
Keep the source-commit/image mapping; a checkout alone does not reproduce every
pinned production image.

## Storage recovery

`iot_postgres-rootdisk-rollback-20260916` and the former named volume are old
copies. Live writes have continued since their snapshot; restarting them would
discard newer measurements. A data restore needs a reviewed reconciliation
covering all newer data and current consumers.

Completed migration records remain in `RUNBOOK.md` and
`DEPLOYMENT-20260916.md`; their pre-cutover commands/hashes are historical.
Use [check-compose.py](check-compose.py) for the maintained ownership validator
and [current recovery requirements](../shelly-compact/RECOVERY.md) for application
storage/time compatibility.
