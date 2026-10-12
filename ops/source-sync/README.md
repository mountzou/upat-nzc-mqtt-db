# Source checkout synchronization

The one-off synchronization completed on 2026-10-05. Its controller, tests and
verification records remain in Git history.

Each synchronization needs a new per-file plan against the current VPS checkout
and a committed target revision. Verify current file hashes, preserve unrelated
local edits and untracked files, and resolve overlapping edits with three-way
merges. Git index/ref updates must preserve local changes and allow recovery of
the previous source and Git state.

Preserve `.env`, production Compose, the development Compose symlink, Caddy and
Mosquitto configuration, operational scripts, systemd sources and job release
configuration. Validate Compose against the
[PostgreSQL ownership contract](../postgres-volume/COMPOSE-OWNERSHIP.md).

Source synchronization copies bootstrap and migration SQL as files; it does not
execute them or activate application changes. Builds, application deployments
and database migrations are separate operations.

Before acceptance, verify planned file hashes, preservation of unrelated files,
internal API health, fresh compact measurements and unchanged database identity.
Source-only synchronization keeps container IDs, images, startup times and restart
counts unchanged.
