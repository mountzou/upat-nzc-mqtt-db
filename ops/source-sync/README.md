# Source checkout synchronization, 2026-10-05

Target source: merged `main` commit `fff3066dcc8afe25a1154cabf96cbbdc58611d36`.
The source checkout on the VPS starts at `b88270e0b5d6a6d8aebd0428656c2e30c8f0f74c`.
The active API and Shelly ingestor application files already match the target commit.

A separately reviewed per-file plan at
`/opt/schoolheroz-source-sync-20261005-fff3066/plan.json` contains 120 source
updates and preserves 28 local deployment, operational, or unrelated overrides.
Its SHA-256 is `385a9b35091ae53cbbc73137f7024108f3c2edad7a7fd68d161fe60171eae045`.
Conflicting unrelated files retain their current contents. Clean three-way
merges preserve the local additions. Unknown untracked files are retained.

The controller verifies the reviewed plan, active application hashes, Git
ancestry, unstaged local work, Compose ownership, database identity and health.
It saves private recovery files and the previous Git index before changes. It
updates only the plan paths, then advances the Git index and main ref without
checking out over local overrides. Failures after source changes restore the
source files and Git state automatically. Backups are never replaced on rerun.

The live `.env`, production Compose, development Compose symlink, Caddy and
Mosquitto configs, operational guard and health scripts, systemd source files,
and job release Compose files are protected. Bootstrap/migration SQL files are
updated as source only; the controller never executes database writes,
migrations, builds, deployments, container restarts, or systemd changes.
Read-only SQL checks verify continuing compact ingestion and unchanged database
identity and PostgreSQL start time. All seven core container IDs, images,
start times and restart counts must remain identical.

Run the isolated real-Git tests with:

```sh
python3 -B ops/source-sync/test_sync_20261005.py
```

Copy the committed controller into the private stage directory on the VPS.
A call without `--apply` performs the preflight only. The approved application
uses the same command with `--apply` appended:

```sh
python3 /opt/schoolheroz-source-sync-20261005-fff3066/sync-controller.py \
  --plan-sha256 385a9b35091ae53cbbc73137f7024108f3c2edad7a7fd68d161fe60171eae045
```

Recovery files and acceptance evidence stay in that private stage directory.
No automatic service action is part of source synchronization or recovery.
