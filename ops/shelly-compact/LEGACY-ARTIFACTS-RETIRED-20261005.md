# Legacy migration/build artifact retirement, 2026-10-05

Result: **PASS**. The committed controller from `dd28853` applied the reviewed
inventory on the VPS. All 71 obsolete files are absent: the 44 previously
identified helpers/build files, 26 additional inactive source/test/cache
snapshots, and the incompatible UTC-naive migration 014 rollback SQL.
They accounted for 786,308 content bytes and 929,792 allocated bytes before
removal. These are file sizes, not a net filesystem-free-space measurement.

The cleanup preserved all 72 files of historical evidence. Three archived
runbooks received a retirement notice; their original bodies retain the exact
prior SHA-256 after that prefix is stripped. Seven stage roots now contain a
RETIRED.md marker. Receipts, original Compose snapshots, manifests and backup
gates remain as dated evidence. Original hashes are in the
[inventory](LEGACY-ARTIFACT-INVENTORY-20261005.json).

Eight canonical documents were updated from the committed revision. Current
recovery keeps compact-only storage, the independent public sequence and
TIMESTAMPTZ. September reverse-copy and UTC-naive database rollback paths are
obsolete. Numbered forward migrations and current bootstrap files were retained.
See [current recovery guidance](RECOVERY.md).

Before deletion the controller rechecked hashes, regular-file ownership,
Docker mounts/labels, systemd/cron/script references, open file descriptors,
process arguments and active builds. No active dependency was found. The
controller checked every existing tracked/untracked checkout file, plus .env,
and preserved all unrelated source/configuration bytes and metadata.

All seven core containers retained their IDs, images, start times and restart
counts. Docker image IDs and volume names were unchanged. PostgreSQL retained
its start time and compact/sequence identities; legacy stayed absent. Internal
API health and the production Compose ownership guard passed, and fresh compact
measurements were observed. No database writes, migrations, builds, container
restarts or scheduler changes were performed by the cleanup.

The full [acceptance receipt](LEGACY-ARTIFACTS-RETIRED-20261005.json) contains the
exact identities and checks. Private inventory, progress and receipts remain
under `/opt/schoolheroz-legacy-cleanup-20261005/` on the VPS. The active October
overlay rollback materials and unrelated dirty VPS/local changes were retained.
