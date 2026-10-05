# Verified production-runtime candidates, 2026-10-05

The final rollout candidates supersede the initial rebuild candidates. Inspection
found that the latter refreshed Python and unpinned runtime packages. These final
candidates instead inherit the exact currently deployed image layers and overlay
only the agreed source files; no dependency installation is performed.

Recipe commit: `0ac942b9b89e9fc5b46cc8ff440fa9788157315c`.
Application source commit: `cf58dd02b791dc14903e0e39c7041ffcc00d56f8`.

- **api**: `schoolheroz-api:compact-only-overlay-0ac942b9b89e`
  - Candidate: `sha256:c1d1970522a08e90b571313146da7ce9b725e3341120ad424f35bdfc25c7218a`.
  - Exact live base: `sha256:ae28e0f1b4e4207455f32bb49ec56a4ac78706ba788181fbc6a9d66bdc4e1a18`.
  - Changed application files: `readers/shelly_storage.py`.
- **ingestor**: `schoolheroz-shelly-ingestor:compact-only-overlay-0ac942b9b89e`
  - Candidate: `sha256:6d3e2f29bc682be29fa91cdeebc925408eceed33afe7aeea56fa851a4bf1aac9`.
  - Exact live base: `sha256:f92a082e0fbbb5cbbd4f3ab922b589df1ca22b403d4f291bbd8f30ace3308d7b`.
  - Changed application files: `main.py`, `measurements.py`.

## Completed checks

- Docker-loaded base identities match the observed live image identities.
- All Python/JSON application files were compared before and after. Only the
  reader selector and the two agreed ingestor files differ.
- Dependency rootfs layers remain the exact prefix of the candidates. Python and
  inspected package versions match the live services exactly; command, entrypoint,
  environment, working directory and user are unchanged.
- A fresh local PostgreSQL bootstrap passed. The ingestor candidate generated
  eight direct rows and processed the real callback path, yielding 11 compact
  measurements plus one raw message and one counter row.
- The API candidate ran its real HTTP server with health/database connected;
  history and latest matched with values [0.0, 286.5].
- The source/test suite already passed 152 tests and 5 subtests before image builds.

## Concrete production change and rollback

A private copy of the exact current production Compose was prepared locally.
Its candidate changes only two image references and removes three mode selectors;
all other parsed fields remain identical. Both originals and candidate parse with
Docker Compose. The original file is the rollback configuration: it restores the
old image IDs and compact/decimal-one selectors. The existing images remain on VPS.
Neither init.sql nor migration 014 is part of this application rollout.

Proposed execution: load the verified prepared images, confirm the live Compose
hash has not changed, preserve the exact original file, install the targeted
candidate and recreate only api and shelly-ingestor with --no-deps --no-build
--pull never. Verify HTTP health/history, fresh persisted compact rows, error logs
and unchanged PostgreSQL/other service identities afterward. Roll back the two
services with the original Compose if acceptance fails.

API and ingestor recreation causes a brief API/collection interruption. The Shelly
subscriber uses QoS 0; delivery of messages published during disconnection cannot
be claimed. This rollout remains pending explicit activation approval.

The 44 obsolete VPS files, unrelated dirty local files and all other services
remain outside this application rollout. No services or configuration on VPS were
changed while preparing these candidates.

Evidence: [VERIFIED-CANDIDATES-20261005.json](VERIFIED-CANDIDATES-20261005.json).
