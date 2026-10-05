# Historical stage directories removed, 2026-10-05

Result: **PASS**. The seven exact `/opt/upat-shelly-compact-stage*` release roots
were deleted after the user explicitly requested their removal. All 79 remaining
historical receipts/manifests/docs/Compose snapshots and empty child directories
were removed. No artifact payload backup was created.

The committed controller `f358ff9` was bound to private inventory SHA-256
`917e71dec56458f9591b125cfdbadb41ffce96b5cdb9442443f91f0a6931d94d`.
It verified exact directory contents, file hashes, identities and ownership,
absence of symlinks and mount points, and no Docker/systemd/cron/process/open-file
references. It unlinked only inventoried regular files and used rmdir, which
refuses unknown additions, for the directories.

The deleted entries accounted for 322,533 content bytes and 647,168 allocated
bytes including directories. These are inventory sizes, not a net filesystem
free-space measurement. The metadata inventory and acceptance evidence remain
at `/opt/schoolheroz-stage-directories-retirement-20261005/`; they contain no
archived artifact payloads.

The production source/config checkout and unrelated dirty files retained their
exact hashes/modes/owners during deletion. All seven core containers retained
IDs, images, start times and restart counts. Docker images and volumes stayed
unchanged; PostgreSQL retained its start time and compact/sequence identities.
Internal API health and the Compose ownership guard passed, and new compact
measurements were observed. No database writes, migrations, builds, restarts or
scheduler changes were performed by the retirement controller.

See the [acceptance receipt](STAGE-DIRECTORIES-REMOVED-20261005.json).
Canonical dated migration/retirement evidence remains in Git. Current October
application rollback materials, other VPS directories and the active independent
sequence were retained. [Current recovery guidance](RECOVERY.md) still applies.
