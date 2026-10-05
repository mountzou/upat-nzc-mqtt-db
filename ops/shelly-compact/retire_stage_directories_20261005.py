"""Delete only the seven approved, fingerprinted historical stage directories."""
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
STAGE = Path('/opt/schoolheroz-stage-directories-retirement-20261005')
BASE = '77b1f249b446d00ddaba24e1a72c0e048ec793c0'
INVENTORY_SHA = '917e71dec56458f9591b125cfdbadb41ffce96b5cdb9442443f91f0a6931d94d'
ROOTS = {
    '/opt/upat-shelly-compact-stage1-20260916',
    '/opt/upat-shelly-compact-stage2-20260916',
    '/opt/upat-shelly-compact-stage2-20260916-r2',
    '/opt/upat-shelly-compact-stage2-20260916-r3',
    '/opt/upat-shelly-compact-stage2-20260916-r4',
    '/opt/upat-shelly-compact-stage3-20260916',
    '/opt/upat-shelly-compact-stage3-20260916-r2',
}


def load_helper(name, relative):
    path = ROOT / relative
    committed = subprocess.check_output(['git', 'show', BASE + ':' + relative], cwd=ROOT)
    assert path.read_bytes() == committed, 'Helper source changed: ' + relative
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # Guarded mains are not executed.
    return module


def fingerprint(row, directory=False):
    p = Path(row['path'])
    assert p.resolve() == p and any(p == Path(r) or p.is_relative_to(Path(r)) for r in ROOTS)
    s = p.lstat()
    assert not stat.S_ISLNK(s.st_mode)
    assert (s.st_dev, s.st_ino, s.st_uid, s.st_gid, stat.S_IMODE(s.st_mode)) == (
        row['device'], row['inode'], row['uid'], row['gid'], row['mode']), str(p)
    if directory:
        assert stat.S_ISDIR(s.st_mode), str(p)
    else:
        assert stat.S_ISREG(s.st_mode) and s.st_nlink == 1, str(p)
        assert s.st_size == row['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest() == row['sha256'], str(p)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    data = (STAGE / 'inventory.json').read_bytes()
    assert hashlib.sha256(data).hexdigest() == INVENTORY_SHA
    inventory = json.loads(data)
    assert set(inventory['release_roots']) == ROOTS and len(inventory['files']) == 79
    assert not (STAGE / 'accepted.json').exists(), 'Already completed'
    c = load_helper('artifact_checks', 'ops/shelly-compact/cleanup_20261005.py')
    m = load_helper('source_checks', 'ops/source-sync/sync_20261005.py')
    assert m.git('rev-parse', 'HEAD').decode().strip() == BASE
    assert not m.git('diff', '--cached', '--name-only')
    actual = {str(p) for r in ROOTS for p in [Path(r), *Path(r).rglob('*')]}
    expected = {row['path'] for row in inventory['files'] + inventory['directories']}
    assert actual == expected, 'Unexpected directory contents'
    for row in inventory['files']:
        fingerprint(row)
    for row in inventory['directories']:
        fingerprint(row, True)
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        mount = line.split()[4]
        assert not any(mount == r or mount.startswith(r + '/') for r in ROOTS), mount
    refs = c.active_references(sorted(ROOTS), [row['path'] for row in inventory['files']])
    assert not refs, refs
    before_containers = m.inspect()
    assert all(s['status'] == 'running' for s in before_containers.values())
    before_images_volumes = c.image_volume_ids()
    before_db = m.database()
    assert before_db['legacy_table'] is None and (before_db['sequence_oid'], before_db['compact_oid']) == (16445, 121068)
    assert m.health() == {'status': 200, 'body': {'status': 'ok', 'database': 'connected'}}
    paths = set(m.git('ls-files', '-z').decode().split('\0'))
    paths.update(m.git('ls-files', '--others', '--exclude-standard', '-z').decode().split('\0'))
    paths.add('.env'); paths.discard('')
    before_files = {p: m.state(p) for p in paths}
    print(json.dumps({'phase': 'preflight_passed', 'directories': 7, 'files': 79,
                      'content_bytes': inventory['bytes'], 'active_references': refs}), flush=True)
    if not args.apply:
        return
    c.save(STAGE / 'before.json', {'containers': before_containers, 'database': before_db,
                                 'images_volumes': before_images_volumes, 'checkout_files': before_files})
    # Remove only inventoried regular files. rmdir refuses any unexpected additions.
    removed = []
    for row in inventory['files']:
        fingerprint(row)
        Path(row['path']).unlink()
        removed.append(row['path'])
        c.save(STAGE / 'progress.json', {'removed_files': removed})
    for row in sorted(inventory['directories'], key=lambda r: len(Path(r['path']).parts), reverse=True):
        fingerprint(row, True)
        Path(row['path']).rmdir()
    assert all(not os.path.lexists(r) for r in ROOTS)
    assert {p: m.state(p) for p in paths} == before_files
    assert m.inspect() == before_containers and c.image_volume_ids() == before_images_volumes
    after_db = m.database()
    for _ in range(20):
        if after_db['latest']['id'] > before_db['latest']['id']:
            break
        time.sleep(1); after_db = m.database()
    assert after_db['latest']['id'] > before_db['latest']['id']
    for key in ['postgres_started_at', 'sequence_oid', 'compact_oid', 'legacy_table']:
        assert after_db[key] == before_db[key], key
    health = m.health()
    assert health == {'status': 200, 'body': {'status': 'ok', 'database': 'connected'}}
    validator = json.loads(m.run(['python3', str(ROOT / 'ops/postgres-volume/check-compose.py'), '--check-default']))
    assert validator['status'] == 'PASS'
    assert m.inspect() == before_containers
    receipt = {'status': 'PASS', 'verified_at_utc': datetime.now(timezone.utc).isoformat(),
               'controller_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               'inventory_sha256': INVENTORY_SHA, 'removed_directories': sorted(ROOTS),
               'removed_file_count': len(removed), 'removed_content_bytes': inventory['bytes'],
               'removed_allocated_bytes': inventory['allocated_bytes'], 'all_directories_absent': True,
               'protected_checkout_files_checked': len(paths), 'checkout_unchanged': True,
               'containers_before': before_containers, 'containers_after': m.inspect(),
               'database_before': before_db, 'database_after': after_db,
               'images_and_volumes_unchanged': c.image_volume_ids() == before_images_volumes,
               'health': health, 'compose_validator': validator, 'database_writes': False,
               'restarts': False, 'artifact_payload_backups_created': False}
    c.save(STAGE / 'accepted.json', receipt)
    print(json.dumps({'status': 'PASS', 'removed_directories': 7, 'removed_files': 79,
                      'checkout_and_services_unchanged': True, 'latest_id': after_db['latest']['id'],
                      'receipt': str(STAGE / 'accepted.json')}), flush=True)


if __name__ == '__main__':
    main()
