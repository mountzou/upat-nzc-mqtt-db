#!/usr/bin/env python3
"""Read-only host guard. Does not mount, initialize, copy, or start anything."""
import json, os, pathlib, subprocess, sys

def require(condition, reason):
    # Safety checks must remain active even if Python runs with optimization.
    if not condition:
        raise ValueError(reason)

def output(*args):
    return subprocess.check_output(args, text=True).strip()

def check(spec):
    mount = pathlib.Path(spec['mountpoint'])
    data = pathlib.Path(spec['data_path'])
    require(mount.is_mount() and not mount.is_symlink(), 'required volume is not mounted')
    require(data.parent == mount and data.name == 'pgdata', 'unexpected data location')
    require(data.is_dir() and not data.is_symlink(), 'copied data directory missing')
    row = json.loads(output('findmnt', '-J', '--mountpoint', str(mount), '-o', 'TARGET,SOURCE,FSTYPE,UUID'))['filesystems'][0]
    require(row['target'] == str(mount) and row['fstype'] == 'ext4', 'wrong mount/filesystem')
    require(row['uuid'] == spec['filesystem_uuid'], 'wrong filesystem UUID')
    require(os.path.samefile(row['source'], spec['device']), 'wrong block device')
    require(output('lsblk', '-dn', '-o', 'SERIAL', spec['device']) == spec['volume_id'], 'wrong volume serial')
    require(output('stat', '-f', '-c', '%i', str(data)) == spec['filesystem_id'], 'wrong filesystem ID')
    require(data.stat().st_uid == spec['uid'] and data.stat().st_gid == spec['gid'], 'wrong owner')
    require(data.stat().st_mode & 0o777 in (0o700, 0o750), 'unsafe data permissions')
    require((data/'PG_VERSION').read_text().strip() == spec['major'], 'wrong major version')
    c = json.loads(output('docker', 'inspect', spec['container']))[0]
    require(c['Image'] == spec['image'], 'wrong container image')
    require(c['HostConfig']['RestartPolicy']['Name'] == 'no', 'unguarded automatic Docker restart')
    m = next(x for x in c['Mounts'] if x['Destination'] == '/var/lib/postgresql/data')
    require(m['Type'] == 'bind' and pathlib.Path(m['Source']) == data and m['RW'], 'wrong data mount')
    require(c['Config']['Entrypoint'] == ['/bin/bash', '/usr/local/libexec/pg-volume-guard.sh'], 'guard bypassed')
    env = dict(x.split('=', 1) for x in c['Config']['Env'])
    for key, value in {'PG_EXPECT_FSID':spec['filesystem_id'], 'PG_EXPECT_SYSTEM_ID':spec['system_id'], 'PG_EXPECT_MAJOR':spec['major']}.items():
        require(env.get(key) == value, 'wrong startup identity: ' + key)
    return {'status':'PASS', 'volume_id':spec['volume_id'], 'container':spec['container']}

if __name__ == '__main__':
    try:
        print(json.dumps(check(json.loads(pathlib.Path(sys.argv[1]).read_text()))))
    except Exception as exc:
        print('PG_VOLUME_HOST_GUARD: ' + str(exc), file=sys.stderr)
        sys.exit(78)
