#!/usr/bin/env python3
"""Offline renderer only. Input/output include secrets: keep both private.
Never contacts Docker or starts/stops a container. Fresh inspect is required at cutover.
"""
import copy, json, os, pathlib, sys

def require(condition, reason):
    # Safety checks must remain active even if Python runs with optimization.
    if not condition:
        raise ValueError(reason)

def render(source, spec):
    require(source['Id'] == spec['source_container_id'], 'source container changed')
    require(source['Image'] == spec['image'] and spec['image'].startswith('sha256:'), 'image changed')
    require(source['Config']['Entrypoint'] == ['docker-entrypoint.sh'], 'unexpected source runtime configuration')
    require(source['Config']['StopSignal'] == 'SIGINT', 'unexpected source runtime configuration')
    require(source['HostConfig']['RestartPolicy']['Name'] == 'no', 'unexpected source runtime configuration')
    payload = copy.deepcopy(source['Config'])
    payload['Hostname'] = ''  # Docker generates a fresh hostname; never reuse source container ID.
    payload['Image'] = spec['image']
    payload['Entrypoint'] = ['/bin/bash', '/usr/local/libexec/pg-volume-guard.sh']
    # Avoid generic Compose reverting the storage mapping during a later deployment.
    payload['Labels'] = {k:v for k,v in payload.get('Labels', {}).items() if not k.startswith('com.docker.compose.')}
    payload['Labels']['upat.storage-owner'] = 'upat-postgres-volume.service'
    require(not any(v.startswith('PG_EXPECT_') for v in payload['Env']), 'unexpected source runtime configuration')
    payload['Env'] += ['PG_EXPECT_FSID='+spec['filesystem_id'], 'PG_EXPECT_SYSTEM_ID='+spec['system_id'], 'PG_EXPECT_MAJOR='+spec['major']]
    host = copy.deepcopy(source['HostConfig'])
    binds = host.get('Binds') or []
    old = [b for b in binds if b.split(':')[1] == '/var/lib/postgresql/data']
    mounts = host.get('Mounts') or []
    old_structured = [m for m in mounts if m['Target'] == '/var/lib/postgresql/data']
    require(len(old) + len(old_structured) == 1, 'expected exactly one data mount')
    host['Binds'] = [b for b in binds if b not in old]
    host['Mounts'] = [m for m in mounts if m not in old_structured] + [
        {'Type':'bind', 'Source':spec['data_path'], 'Target':'/var/lib/postgresql/data', 'ReadOnly':False},
        {'Type':'bind', 'Source':spec['guard_path'], 'Target':'/usr/local/libexec/pg-volume-guard.sh', 'ReadOnly':True}
    ]
    # Structured --mount semantics reject missing host paths; do not use bind-create-src.
    host['RestartPolicy'] = {'Name':'no', 'MaximumRetryCount':0}
    payload['HostConfig'] = host
    payload['NetworkingConfig'] = {'EndpointsConfig':{}}
    for name, endpoint in source['NetworkSettings']['Networks'].items():
        if name in ('none', 'host', 'bridge'):
            continue
        require(not endpoint.get('IPAMConfig'), 'static IP needs explicit handoff plan')
        aliases = [a for a in (endpoint.get('Aliases') or []) if a not in (source['Id'], source['Id'][:12])]
        payload['NetworkingConfig']['EndpointsConfig'][name] = {'Aliases':aliases}
    return payload

if __name__ == '__main__':
    os.umask(0o077)
    values = json.loads(pathlib.Path(sys.argv[1]).read_text())
    if isinstance(values, list):
        values = next(c for c in values if c['Name']=='/iot_postgres')
    result = render(values, json.loads(pathlib.Path(sys.argv[2]).read_text()))
    with pathlib.Path(sys.argv[3]).open('x') as f:
        json.dump(result, f, indent=2)
        f.write('\n')
    print('Private create payload prepared; no Docker action performed.')
