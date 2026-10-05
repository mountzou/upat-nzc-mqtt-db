"""One-off, manifest-bound retirement of obsolete Shelly stage files and guidance."""
import argparse
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path('/opt/upat-nzc-mqtt-db')
STAGE = Path('/opt/schoolheroz-legacy-cleanup-20261005')
BASE = 'fff3066dcc8afe25a1154cabf96cbbdc58611d36'
INVENTORY_SHA = '05e8b428950a6ae6986ec4150bbc5e8fcdff1d93f6cf100aa2689751bfb84c0c'
HELPER_SHA = 'e12893650fa84e05938f23bd303594b05a274a6ce458a197afd162cad2422480'
README_CURRENT_BLOB = 'd0c13572ea79e8ba3ad9a4de02a38bc2cce1b077'
DOCS = ['db/maintenance/README.md', 'ops/TIME_PERSISTENCE.md', 'ops/TIME_CONSUMERS.md',
        'ops/shelly-compact/README.md', 'ops/shelly-compact/RUNBOOK.md',
        'ops/shelly-compact/DEPLOYMENT-STAGE2-20260916.md',
        'ops/shelly-compact/PRODUCTION-STAGE3-20260916.md', 'ops/shelly-compact/RECOVERY.md']
NOTICE = (b'> Historical migration evidence; retired on 2026-10-05.\n'
          b'> The legacy table and reverse-copy recovery no longer exist. Do not run\n'
          b'> this archived procedure against the current database. Current guidance:\n'
          b'> `/opt/upat-nzc-mqtt-db/ops/shelly-compact/RECOVERY.md`.\n\n')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def emit(value):
    print(json.dumps(value), flush=True)


def save(path, value):
    path.write_text(json.dumps(value, indent=2))
    path.chmod(0o600)


def check_file(row):
    p = Path(row['path'])
    assert p.resolve() == p and not p.is_symlink(), str(p)
    s = p.lstat()
    assert stat.S_ISREG(s.st_mode) and s.st_nlink == 1, str(p)
    assert s.st_size == row['bytes'] and sha(p.read_bytes()) == row['sha256'], str(p)


def active_references(roots, candidate_names):
    refs = []
    ids = subprocess.check_output(['docker', 'ps', '-aq'], text=True).split()
    containers = json.loads(subprocess.check_output(['docker', 'inspect', *ids], text=True))
    for c in containers:
        for mount in c['Mounts']:
            source = Path(mount['Source'])
            if any(Path(r).is_relative_to(source) or source.is_relative_to(Path(r)) for r in roots):
                refs.append('mount:' + c['Name'])
            if any(Path(p).is_relative_to(source) for p in candidate_names):
                refs.append('candidate_mount:' + c['Name'])
        for value in (c['Config']['Labels'] or {}).values():
            if any(r in value for r in roots):
                refs.append('container_label:' + c['Name'])
    config_dirs = ['/etc/systemd/system', '/usr/lib/systemd/system', '/etc/cron.d',
                   '/var/spool/cron/crontabs', '/usr/local/bin', '/usr/local/libexec']
    terms = roots + candidate_names
    for directory in config_dirs:
        for p in Path(directory).rglob('*'):
            if p.is_file() and p.stat().st_size < 1024 * 1024:
                try:
                    data = p.read_text()
                except (UnicodeError, OSError):
                    continue
                if any(term in data for term in terms) or 'rollback_014_telemetry_timestamptz.sql' in data:
                    refs.append('config:' + str(p))
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            command = (proc / 'cmdline').read_bytes().decode(errors='replace').replace('\0', ' ')
            if any(term in command for term in terms):
                refs.append('process:' + proc.name)
            if ('docker build ' in command or 'docker buildx build ' in command):
                refs.append('active_build:' + proc.name)
        except OSError:
            pass
        for entry in [proc / 'cwd', proc / 'exe', *list((proc / 'fd').glob('*'))]:
            try:
                path = os.readlink(entry)
            except OSError:
                continue
            if any(path == r or path.startswith(r + '/') for r in roots) or path in candidate_names:
                refs.append('open:' + proc.name)
    return sorted(set(refs))


def image_volume_ids():
    return {key: sorted(set(subprocess.check_output(args, text=True).split()))
            for key, args in [('images', ['docker', 'image', 'ls', '-q', '--no-trunc']),
                              ('volumes', ['docker', 'volume', 'ls', '-q'])]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--revision', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    inventory_bytes = (STAGE / 'inventory.json').read_bytes()
    assert sha(inventory_bytes) == INVENTORY_SHA
    inventory = json.loads(inventory_bytes)
    roots = inventory['release_roots']
    assert len(roots) == 7 and all(r.startswith('/opt/upat-shelly-compact-stage') for r in roots)
    candidates = inventory['candidates']
    assert len(candidates) == 71
    names = [row['path'] for row in candidates]
    assert len(set(names)) == len(names)
    canonical = {str(ROOT / 'ops/shelly-compact/Dockerfile.ingestor'),
                 str(ROOT / 'db/maintenance/rollback_014_telemetry_timestamptz.sql')}
    assert all(p in canonical or any(Path(p).is_relative_to(Path(r)) for r in roots) for p in names)
    for row in candidates + inventory['preserved_evidence']:
        check_file(row)
    assert not (STAGE / 'accepted.json').exists(), 'Already applied'
    helper = Path('/opt/schoolheroz-source-sync-20261005-fff3066/sync-controller.py')
    assert sha(helper.read_bytes()) == HELPER_SHA
    spec = importlib.util.spec_from_file_location('verified_source_helpers', helper)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)  # Its guarded main is not executed.
    assert m.git('rev-parse', 'HEAD').decode().strip() == BASE
    assert not m.git('diff', '--cached', '--name-only')
    m.git('merge-base', '--is-ancestor', BASE, args.revision)
    docs = {}
    for relative in DOCS:
        if relative == 'ops/shelly-compact/RECOVERY.md':
            assert m.state(relative) is None
        elif relative == 'ops/shelly-compact/README.md':
            assert m.git('hash-object', relative).decode().strip() == README_CURRENT_BLOB
        else:
            assert m.content(relative) == m.git('show', BASE + ':' + relative), relative
        docs[relative] = m.git('show', args.revision + ':' + relative)
    removed = subprocess.run(['git', 'cat-file', '-e', args.revision + ':db/maintenance/rollback_014_telemetry_timestamptz.sql'],
                             cwd=ROOT, capture_output=True)
    assert removed.returncode != 0, 'Obsolete inverse SQL remains in target revision'
    refs = active_references(roots, names)
    assert not refs, refs
    before_containers = m.inspect()
    before_images_volumes = image_volume_ids()
    before_db = m.database()
    assert before_db['legacy_table'] is None and (before_db['sequence_oid'], before_db['compact_oid']) == (16445, 121068)
    assert m.health() == {'status': 200, 'body': {'status': 'ok', 'database': 'connected'}}
    tracked = m.git('ls-files', '-z').decode().split('\0')
    others = m.git('ls-files', '--others', '--exclude-standard', '-z').decode().split('\0')
    checked = set(tracked + others + DOCS + ['.env']); checked.discard('')
    expected = {p: m.state(p) for p in sorted(checked)}
    before_states = dict(expected)
    annotated = [row for row in inventory['preserved_evidence'] if Path(row['path']).name == 'RUNBOOK.md']
    assert len(annotated) == 3
    assert all(not (Path(r) / 'RETIRED.md').exists() for r in roots)
    emit({'phase': 'preflight_passed', 'delete_files': len(candidates), 'bytes': sum(r['bytes'] for r in candidates),
          'docs_to_update': len(DOCS), 'preserved_evidence': len(inventory['preserved_evidence']), 'active_references': refs})
    if not args.apply:
        return
    save(STAGE / 'before.json', {'containers': before_containers, 'database': before_db,
                               'images_volumes': before_images_volumes, 'checkout_files': before_states,
                               'source_revision': args.revision, 'inventory_sha256': INVENTORY_SHA})
    created_dirs = set()
    for relative, data in docs.items():
        previous = expected[relative]
        metadata = {'kind': 'file', 'sha256': sha(data), 'mode': previous['mode'] if previous else 0o644,
                    'uid': previous['uid'] if previous else os.getuid(), 'gid': previous['gid'] if previous else os.getgid()}
        assert m.state(relative) == previous
        m.install(relative, data, metadata, created_dirs)
        expected[relative] = metadata
    for row in annotated:
        p = Path(row['path']); check_file(row)
        p.write_bytes(NOTICE + p.read_bytes())
    for r in roots:
        (Path(r) / 'RETIRED.md').write_text('# Retired migration stage — 2026-10-05\n\n'
            'Obsolete controllers, build/source snapshots, tests and Python caches were removed.\n'
            'Receipts, manifests, backup gates and dated evidence remain.\n'
            'Archived Compose/checkpoints are historical evidence, not a current rollback.\n'
            'Current recovery guidance: /opt/upat-nzc-mqtt-db/ops/shelly-compact/RECOVERY.md\n')
    deleted = []
    for row in candidates:
        check_file(row)
        p = Path(row['path']); p.unlink(); deleted.append(str(p))
        if p.is_relative_to(ROOT):
            expected[str(p.relative_to(ROOT))] = None
        save(STAGE / 'progress.json', {'source_revision': args.revision, 'deleted': deleted})
    for r in roots:
        for directory in sorted([p for p in Path(r).rglob('*') if p.is_dir()], key=lambda p: len(p.parts), reverse=True):
            try:
                directory.rmdir()
            except OSError:
                pass
    assert all(not Path(p).exists() for p in names)
    for row in inventory['preserved_evidence']:
        data = Path(row['path']).read_bytes()
        if Path(row['path']).name == 'RUNBOOK.md':
            assert data.startswith(NOTICE); data = data[len(NOTICE):]
        assert sha(data) == row['sha256'], row['path']
    assert {p: m.state(p) for p in checked} == expected
    assert m.inspect() == before_containers and image_volume_ids() == before_images_volumes
    health = m.health()
    assert health == {'status': 200, 'body': {'status': 'ok', 'database': 'connected'}}
    after_db = m.database()
    for _ in range(20):
        if after_db['latest']['id'] > before_db['latest']['id']:
            break
        time.sleep(1); after_db = m.database()
    assert after_db['latest']['id'] > before_db['latest']['id']
    for key in ['postgres_started_at', 'legacy_table', 'sequence_oid', 'compact_oid']:
        assert after_db[key] == before_db[key], key
    validator = json.loads(m.run(['python3', str(ROOT / 'ops/postgres-volume/check-compose.py'), '--check-default']))
    assert validator['status'] == 'PASS'
    receipt = {'status': 'PASS', 'verified_at_utc': datetime.now(timezone.utc).isoformat(), 'source_revision': args.revision,
               'controller_sha256': sha(Path(__file__).read_bytes()), 'inventory_sha256': INVENTORY_SHA,
               'deleted_files': deleted, 'removed_bytes': sum(row['bytes'] for row in candidates),
               'removed_allocated_bytes': sum(row['allocated_bytes'] for row in candidates),
               'docs_updated': DOCS, 'historical_runbooks_annotated': len(annotated),
               'preserved_evidence_files': len(inventory['preserved_evidence']),
               'protected_checkout_files_checked': len(checked), 'containers_before': before_containers,
               'containers_after': m.inspect(), 'database_before': before_db, 'database_after': after_db,
               'images_and_volumes_unchanged': image_volume_ids() == before_images_volumes, 'health': health,
               'compose_validator': validator, 'database_writes': False, 'restarts': False}
    save(STAGE / 'accepted.json', receipt)
    emit({'status': 'PASS', 'deleted_files': len(deleted), 'removed_bytes': receipt['removed_bytes'],
          'removed_allocated_bytes': receipt['removed_allocated_bytes'], 'preserved_evidence_files': 72,
          'containers_images_volumes_unchanged': True, 'latest_id': after_db['latest']['id'],
          'receipt': str(STAGE / 'accepted.json')})


if __name__ == '__main__':
    main()
