"""Guarded source-only sync; the reviewed plan is prepared separately."""
import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path('/opt/upat-nzc-mqtt-db')
STAGE = Path('/opt/schoolheroz-source-sync-20261005-fff3066')
TARGET = 'fff3066dcc8afe25a1154cabf96cbbdc58611d36'
COMPOSE_SHA = '86bb4e9c0370429ab7e9b0f82a3e184ccaa1bc26b343d76351d2a99d7fe9ed69'
CONTAINERS = ['iot_api', 'shelly_ingestor', 'iot_postgres', 'nzc_energyplus',
              'iot_mosquitto', 'iot_caddy', 'ttn_ingestor']
PROTECTED = ['.env', 'docker-compose.yml', 'docker-compose.prod.yml',
             'caddy/Caddyfile', 'mqtt/mosquitto/mosquitto.conf',
             'ops/postgres-volume/check-compose.py', 'ops/run-energy-aggregator.sh',
             'ops/vps-health-check.sh']


def run(args, data=None):
    return subprocess.check_output(args, input=data, cwd=ROOT, timeout=60)


def git(*args):
    return run(['git', *args])


def emit(value):
    print(json.dumps(value), flush=True)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def safe_path(relative):
    p = Path(relative)
    if p.is_absolute() or '..' in p.parts or p.parts[0] == '.git':
        raise RuntimeError('Unsafe checkout path')
    full = ROOT / p
    if not full.parent.resolve().is_relative_to(ROOT.resolve()):
        raise RuntimeError('Checkout parent escapes through a symlink: ' + relative)
    return full


def state(relative):
    p = safe_path(relative)
    if not p.exists() and not p.is_symlink():
        return None
    s = p.lstat()
    if stat.S_ISLNK(s.st_mode):
        kind, content = 'symlink', os.readlink(p).encode()
    elif stat.S_ISREG(s.st_mode):
        kind, content = 'file', p.read_bytes()
    else:
        raise RuntimeError('Non-file collision: ' + relative)
    return {'kind': kind, 'sha256': sha(content), 'mode': stat.S_IMODE(s.st_mode),
            'uid': s.st_uid, 'gid': s.st_gid}


def content(relative):
    p = safe_path(relative)
    return os.readlink(p).encode() if p.is_symlink() else p.read_bytes()


def install(relative, data, metadata, created_dirs):
    p = safe_path(relative)
    parent = p.parent
    while parent != ROOT and not parent.exists():
        created_dirs.add(str(parent.relative_to(ROOT)))
        parent = parent.parent
    p.parent.mkdir(parents=True, exist_ok=True)
    temp = p.with_name('.' + p.name + '.source-sync.tmp')
    if temp.exists() or temp.is_symlink():
        raise RuntimeError('Temporary file collision: ' + relative)
    try:
        if metadata['kind'] == 'symlink':
            os.symlink(data.decode(), temp)
            os.lchown(temp, metadata['uid'], metadata['gid'])
        else:
            temp.write_bytes(data)
            temp.chmod(metadata['mode'])
            os.chown(temp, metadata['uid'], metadata['gid'])
        os.replace(temp, p)
    finally:
        if temp.exists() or temp.is_symlink():
            temp.unlink()


def inspect():
    rows = json.loads(run(['docker', 'inspect', *CONTAINERS]))
    return {v['Name']: {'id': v['Id'], 'image': v['Image'],
                        'started_at': v['State']['StartedAt'],
                        'status': v['State']['Status'], 'restart_count': v['RestartCount']}
            for v in rows}


def runtime_sources():
    result = {}
    code = ('import json,hashlib; from pathlib import Path; r=Path("/app"); '
            'print(json.dumps({str(p.relative_to(r)):hashlib.sha256(p.read_bytes()).hexdigest() '
            'for p in r.rglob("*") if p.is_file() and p.suffix in (".py",".json")}))')
    for name, prefix in [('iot_api', 'api'), ('shelly_ingestor', 'mqtt/shelly-devices')]:
        rows = json.loads(run(['docker', 'exec', name, 'python', '-B', '-c', code]))
        for path, digest in rows.items():
            relative = prefix + '/' + path
            if git('ls-tree', TARGET, '--', relative):
                assert sha(git('show', TARGET + ':' + relative)) == digest, relative
                result[relative] = digest
    return result


DB_CODE = '''import json,database
c=database.get_connection(); c.set_session(readonly=True)
try:
    with c.cursor() as q:
        q.execute("SET LOCAL statement_timeout='5s'")
        q.execute("SELECT pg_postmaster_start_time() AS postgres_started_at,to_regclass('public.shelly_measurements') AS legacy_table,to_regclass('public.shelly_measurements_id_seq')::oid AS sequence_oid,to_regclass('shelly_compact.measurements')::oid AS compact_oid")
        result=q.fetchone()
        q.execute("SELECT id,event_time FROM shelly_compact.measurements ORDER BY id DESC LIMIT 1")
        result['latest']=q.fetchone()
        print(json.dumps(result,default=str))
    c.rollback()
finally:c.close()
'''


def database():
    return json.loads(run(['docker', 'exec', '-i', 'iot_api', 'python', '-B', '-'],
                          DB_CODE.encode()))


def health():
    code = ('import json,urllib.request; r=urllib.request.urlopen('
            '"http://127.0.0.1:8000/health",timeout=3); '
            'print(json.dumps({"status":r.status,"body":json.loads(r.read())}))')
    return json.loads(run(['docker', 'exec', 'iot_api', 'python', '-B', '-c', code]))


def json_save(path, value):
    path.write_text(json.dumps(value, indent=2))
    path.chmod(0o600)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plan-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    plan_bytes = (STAGE / 'plan.json').read_bytes()
    assert sha(plan_bytes) == args.plan_sha256, 'Reviewed plan changed'
    plan = json.loads(plan_bytes)
    assert plan['target'] == TARGET and not plan['runtime_differences_from_target']
    old = plan['old_head']
    assert git('rev-parse', 'HEAD').decode().strip() == old, 'HEAD changed'
    assert git('rev-parse', 'origin/main').decode().strip() == TARGET
    assert git('symbolic-ref', 'HEAD').decode().strip() == 'refs/heads/main'
    assert not git('diff', '--cached', '--name-only'), 'Pre-existing staged changes'
    git('merge-base', '--is-ancestor', old, TARGET)
    assert state('docker-compose.prod.yml')['sha256'] == COMPOSE_SHA
    assert state('docker-compose.yml')['kind'] == 'symlink'
    assert content('docker-compose.yml') == b'docker-compose.prod.yml'
    before_containers = inspect()
    assert all(v['status'] == 'running' for v in before_containers.values())
    runtime = runtime_sources()
    actions = plan['actions']
    action_names = [a['path'] for a in actions]
    assert len(set(action_names)) == len(action_names)
    assert not any(p in PROTECTED or p.startswith('ops/systemd/') or
                   p.endswith('/compose.release.yml') for p in action_names)
    paths = set(PROTECTED)
    for ref in [old, TARGET]:
        paths.update(git('ls-tree', '-r', '--name-only', '-z', ref).decode().split('\0'))
    paths.update(git('ls-files', '--others', '--exclude-standard', '-z').decode().split('\0'))
    paths.discard('')
    before_files = {p: state(p) for p in sorted(paths)}
    for a in actions:
        s = before_files[a['path']]
        assert (s['sha256'] if s else None) == a['current_sha256'], a['path']
        assert a['operation'] in ('write', 'remove')
        if a['operation'] == 'write':
            result_path = Path(a['result_file'])
            assert result_path.parent == STAGE / 'planned-files'
            assert sha(result_path.read_bytes()) == a['result_sha256'], a['path']
            assert a['result_mode'] in ('100644', '100755', '120000')
    before_db = database()
    assert before_db['legacy_table'] is None
    assert (before_db['sequence_oid'], before_db['compact_oid']) == (16445, 121068)
    assert health() == {'status': 200, 'body': {'status': 'ok', 'database': 'connected'}}
    emit({'phase': 'preflight_passed', 'source_updates': len(actions),
          'preserved_overrides': len(plan['preserved_overrides']), 'runtime_source_files': len(runtime),
          'plan_sha256': args.plan_sha256})
    if not args.apply:
        return

    backup = STAGE / 'backup'
    backup.mkdir(mode=0o700)  # Refuse a second application; recovery evidence is never replaced.
    backups = backup / 'files'
    backups.mkdir(mode=0o700)
    index = Path(git('rev-parse', '--git-path', 'index').decode().strip())
    if not index.is_absolute():
        index = ROOT / index
    index_metadata = index.stat()
    shutil.copyfile(index, backup / 'index')
    (backup / 'index').chmod(0o600)
    (backup / 'status-before.txt').write_bytes(git('status', '--porcelain=v1', '-uall'))
    (backup / 'tracked-diff-before.patch').write_bytes(git('diff', '--binary', '--no-ext-diff'))
    recovery = {}
    for p in sorted(set(action_names) | set(PROTECTED) |
                    {o['path'] for o in plan['preserved_overrides']}):
        recovery[p] = before_files[p]
        if before_files[p] is not None:
            destination = backups / sha(p.encode())
            destination.write_bytes(content(p))
            destination.chmod(0o600)
    json_save(backup / 'files-before.json', before_files)
    json_save(backup / 'recovery.json', {'old_head': old, 'target': TARGET, 'files': recovery})
    json_save(STAGE / 'before.json', {'head': old, 'containers': before_containers,
                                     'database': before_db, 'plan_sha256': args.plan_sha256})
    assert {p: state(p) for p in paths} == before_files, 'Files changed during backup'
    assert inspect() == before_containers, 'Containers changed before source update'
    expected_files = dict(before_files)
    changed = []
    created_dirs = set()
    index_changed = False
    head_changed = False
    try:
        for a in actions:
            p = a['path']
            assert state(p) == before_files[p], p
            changed.append(p)
            if a['operation'] == 'remove':
                if safe_path(p).exists() or safe_path(p).is_symlink():
                    safe_path(p).unlink()
                expected_files[p] = None
            else:
                previous = before_files[p]
                metadata = {'kind': 'symlink' if a['result_mode'] == '120000' else 'file',
                            'mode': 0o777 if a['result_mode'] == '120000' else
                                    (0o755 if a['result_mode'] == '100755' else 0o644),
                            'uid': previous['uid'] if previous else os.getuid(),
                            'gid': previous['gid'] if previous else os.getgid(),
                            'sha256': a['result_sha256']}
                install(p, Path(a['result_file']).read_bytes(), metadata, created_dirs)
                expected_files[p] = metadata
        assert {p: state(p) for p in paths} == expected_files, 'File preservation failed'
        index_changed = True
        git('read-tree', TARGET)  # Updates the index only; never checks out over local files.
        git('update-ref', 'refs/heads/main', TARGET, old)
        head_changed = True
        git('diff', '--cached', '--exit-code', TARGET, '--')
        assert git('rev-parse', 'HEAD').decode().strip() == TARGET
        assert inspect() == before_containers, 'Service state changed'
        assert {p: state(p) for p in paths} == expected_files, 'Unexpected file change'
        for p, digest in runtime.items():
            assert state(p)['sha256'] == digest, p
        for p in ['db/init.sql', 'db/migrations/014_telemetry_timestamptz.sql']:
            assert content(p) == git('show', TARGET + ':' + p), p
        validator = json.loads(run(['python3', str(ROOT / 'ops/postgres-volume/check-compose.py'),
                                    '--check-default']))
        assert validator['status'] == 'PASS', 'Compose ownership guard failed'
        after_health = health()
        assert after_health == {'status': 200, 'body': {'status': 'ok', 'database': 'connected'}}
        after_db = database()
        for _ in range(20):
            if after_db['latest']['id'] > before_db['latest']['id']:
                break
            time.sleep(1)
            after_db = database()
        assert after_db['latest']['id'] > before_db['latest']['id'], 'No fresh ingestion observed'
        for key in ['postgres_started_at', 'legacy_table', 'sequence_oid', 'compact_oid']:
            assert after_db[key] == before_db[key], key
        assert inspect() == before_containers
        assert {p: state(p) for p in paths} == expected_files
        receipt = {'status': 'PASS', 'verified_at_utc': datetime.now(timezone.utc).isoformat(),
                   'old_head': old, 'target_head': TARGET, 'plan_sha256': args.plan_sha256,
                   'controller_sha256': sha(Path(__file__).read_bytes()), 'source_updates': len(actions),
                   'preserved_overrides': plan['preserved_overrides'], 'checked_files': len(paths),
                   'runtime_source_files_matched': len(runtime), 'containers_before': before_containers,
                   'containers_after': inspect(), 'database_before': before_db, 'database_after': after_db,
                   'compose_sha256': state('docker-compose.prod.yml')['sha256'],
                   'dev_compose_symlink': content('docker-compose.yml').decode(),
                   'health': after_health, 'compose_validator': validator,
                   'git_status_after': git('status', '--porcelain=v1', '-uall').decode().splitlines(),
                   'database_write_statements_executed': False, 'containers_restarted': False,
                   'rollback_directory': str(backup)}
        json_save(STAGE / 'accepted.json', receipt)
        emit({'status': 'PASS', 'head': TARGET, 'source_updates': len(actions),
              'preserved_overrides': len(plan['preserved_overrides']), 'checked_files': len(paths),
              'containers_unchanged': True, 'before_latest_id': before_db['latest']['id'],
              'after_latest_id': after_db['latest']['id'], 'receipt': str(STAGE / 'accepted.json')})
    except Exception as error:
        emit({'status': 'ERROR', 'error': str(error), 'rolling_back_source_only': True})
        for p in reversed(changed):
            metadata = recovery[p]
            if metadata is None:
                if safe_path(p).exists() or safe_path(p).is_symlink():
                    safe_path(p).unlink()
            else:
                install(p, (backups / sha(p.encode())).read_bytes(), metadata, created_dirs)
        if head_changed:
            git('update-ref', 'refs/heads/main', old, TARGET)
        if index_changed:
            shutil.copyfile(backup / 'index', index)
            index.chmod(stat.S_IMODE(index_metadata.st_mode))
        for directory in sorted(created_dirs, key=lambda p: len(Path(p).parts), reverse=True):
            try:
                (ROOT / directory).rmdir()
            except OSError:
                pass
        assert {p: state(p) for p in paths} == before_files, 'Rollback verification failed'
        assert git('rev-parse', 'HEAD').decode().strip() == old
        emit({'status': 'SOURCE_ROLLED_BACK', 'head': old})
        raise SystemExit(1)


if __name__ == '__main__':
    main()
