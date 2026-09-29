#!/usr/bin/env python3
"""Read-only check of Compose ownership after the production Volume cutover.

Resolved credentials stay in memory. Only field paths and booleans are emitted.
No build, pull, container lifecycle, network or volume mutation is performed.
"""
import argparse
import copy
import json
from pathlib import Path
import subprocess
import sys

NETWORK = 'upat-nzc-mqtt-db_default'
PGDATA = '/mnt/HC_Volume_106884142/pgdata'


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def output(args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, stderr=subprocess.PIPE, text=True)


def differences(a, b, path=''):
    if isinstance(a, dict) and isinstance(b, dict):
        return [v for k in sorted(a.keys() | b.keys()) for v in
                ([path+'/'+k] if k not in a or k not in b else differences(a[k], b[k], path+'/'+k))]
    return [] if a == b else [path]


def normalize(model):
    # Compose versions differ in whether external networks emit empty IPAM.
    model=copy.deepcopy(model)
    for network in model.get('networks', {}).values():
        if network.get('external') and network.get('ipam') == {}:
            network.pop('ipam')
    return model


def expected_after(before):
    result = copy.deepcopy(before)
    require('postgres' in result['services'], 'baseline must describe the old PostgreSQL service')
    result['services'].pop('postgres')
    for service in result['services'].values():
        deps = service.get('depends_on', {})
        if isinstance(deps, dict):
            deps.pop('postgres', None)
        else:
            deps = [x for x in deps if x != 'postgres']
        if deps:
            service['depends_on'] = deps
        else:
            service.pop('depends_on', None)
    require('postgres_data' in result.get('volumes', {}), 'baseline PostgreSQL volume absent')
    result['volumes'].pop('postgres_data')
    if not result['volumes']:
        result.pop('volumes')
    result.setdefault('networks', {})['default'] = {'name': NETWORK, 'external': True}
    return result


def validate_model(model):
    require('postgres' not in model['services'], 'Compose must not own PostgreSQL')
    require('postgres_data' not in model.get('volumes', {}), 'old database volume is still declared')
    require(model.get('networks', {}).get('default') == {'name': NETWORK, 'external': True}, 'shared network must be external')
    for name, service in model['services'].items():
        require(service.get('container_name') != 'iot_postgres', 'database container name reused')
        require('postgres' not in service.get('depends_on', {}), 'database lifecycle dependency remains')
        for mount in service.get('volumes', []):
            require(mount.get('target') != '/var/lib/postgresql/data', 'PGDATA mounted by Compose')


def stable_state(containers):
    return {c['Name']: {'id':c['Id'], 'image':c['Image'], 'started':c['State']['StartedAt'],
            'state':c['State']['Status'], 'restarts':c['RestartCount'],
            'mounts': sorted(c['Mounts'], key=lambda m:m['Destination']),
            'networks': c['NetworkSettings']['Networks']} for c in containers}


def inspect_all():
    ids=output(['docker','ps','-aq']).split()
    return json.loads(output(['docker','inspect',*ids]))


def compose_config(root, path, all_profiles=True):
    args=['docker','compose','--project-directory',str(root),'--env-file',str(root/'.env'),'-p','upat-nzc-mqtt-db']
    if all_profiles:
        args += ['--profile','*']
    if path is not None:
        args += ['-f',str(path)]
    return normalize(json.loads(output(args+['config','--format','json'],cwd=root)))


def verify(root, candidate, baseline=None, check_default=False):
    before=inspect_all()
    pg=next(c for c in before if c['Name']=='/iot_postgres')
    require(pg['State']['Running'], 'active PostgreSQL is not running')
    require(not (pg['Config'].get('Labels') or {}).get('com.docker.compose.project'), 'active DB must be outside Compose ownership')
    require(not pg['HostConfig'].get('PortBindings'), 'PostgreSQL has published ports')
    require(NETWORK in pg['NetworkSettings']['Networks'], 'database disconnected from shared network')
    require('postgres' in pg['NetworkSettings']['Networks'][NETWORK].get('Aliases', []), 'postgres DNS alias absent')
    require(any(m['Source']==PGDATA and m['Destination']=='/var/lib/postgresql/data' for m in pg['Mounts']), 'wrong PGDATA source')
    output(['python3','/usr/local/libexec/upat-pg-host-preflight.py','/etc/upat-nzc/postgres-volume.json'])
    require(output(['systemctl','is-active','upat-postgres-volume.service']).strip()=='active','database systemd unit inactive')
    output(['docker','exec','iot_postgres','pg_isready','-U','postgres','-d','iot_db'])
    actual=compose_config(root,candidate)
    validate_model(actual)
    if baseline:
        errors=differences(expected_after(compose_config(root,baseline)), actual)
        require(not errors, 'unapproved model differences: '+','.join(errors))
    default=compose_config(root,candidate,False)
    require(default['services']=={k:v for k,v in actual['services'].items() if not v.get('profiles')},'default profile changed')
    if check_default:
        require(compose_config(root,None)==actual, 'default Compose command differs from production')
        require(compose_config(root,root/'docker-compose.yml')==actual, 'default file differs from production')
    # Check the current long-running services, without exposing environment values.
    live={c['Name'].lstrip('/'):c for c in before if c['State']['Running']}
    for name, service in actual['services'].items():
        c=live.get(service.get('container_name'))
        if not c: continue
        env=dict(x.split('=',1) for x in c['Config']['Env'])
        for key,value in service.get('environment',{}).items():
            require(env.get(key)==value,'live environment differs: '+name+'/'+key)
        if service.get('image'):
            require(json.loads(output(['docker','image','inspect',service['image']]))[0]['Id']==c['Image'],'live image differs: '+name)
    require(stable_state(before)==stable_state(inspect_all()),'container state changed during read-only verification')
    return {'status':'PASS','read_only':True,'services':len(actual['services']),
            'postgres_owned_by_systemd':True,'database_volume_absent_from_compose':True,
            'network_external':True,'live_environment_and_images_match':True,
            'only_approved_contract_changes':bool(baseline),'default_entrypoint_checked':check_default,
            'container_state_unchanged':True}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path('/opt/upat-nzc-mqtt-db'))
    parser.add_argument('--candidate',type=Path)
    parser.add_argument('--baseline',type=Path)
    parser.add_argument('--check-default',action='store_true')
    args=parser.parse_args()
    try:
        result=verify(args.root,args.candidate or args.root/'docker-compose.prod.yml',args.baseline,args.check_default)
        print(json.dumps(result,indent=2))
        return 0
    except ValueError as e:
        print(json.dumps({'status':'FAIL','reason':str(e)}));return 1
    except (OSError,subprocess.CalledProcessError,KeyError,StopIteration) as e:
        print(json.dumps({'status':'FAIL','error_type':type(e).__name__,'details':'Private command output suppressed'}));return 1


if __name__=='__main__':sys.exit(main())
