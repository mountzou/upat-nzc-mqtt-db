#!/usr/bin/env python3
"""Approved API-only cutover. No database DDL or ingestor lifecycle operations."""
import argparse
import copy
import datetime as dt
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

STAGE = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('stage1_helpers', STAGE/'ops/shelly-compact/stage1.py')
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)
ROOT = s.ROOT
RECEIPTS = STAGE/'receipts'
PREVIOUS = Path('/opt/upat-shelly-compact-stage1-20260916/receipts')
IMAGE = 'sha256:81ba07c4a526cdd8dbc3b3c0c79457633efbc91cd869be9a28af4befb9796429'
OLD_IMAGE = 'sha256:d27069f42d0f4cb647d8011a19dfbf81a7dcb71a140b493f033ed847eb788856'
APP_COMMIT = s.APP_COMMIT
CANONICAL = ROOT/'docker-compose.prod.yml'
CANDIDATE = STAGE/'ops/shelly-compact/compose.stage2.prod.yml'
require, out, sha = s.require, s.out, s.sha


def save(name, data):
    with (RECEIPTS/name).open('x') as f:
        json.dump(data, f, indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())


def expected_after(before):
    result = copy.deepcopy(before)
    api = result['services']['api']
    api['image'] = IMAGE
    api.setdefault('environment', {}).update(SHELLY_MEASUREMENTS_READ_STORAGE='compact',
                                            SHELLY_MEASUREMENTS_ROUNDING='decimal_1')
    return result


def artifacts():
    manifest = json.loads((STAGE/'ARTIFACT-MANIFEST.json').read_text())
    for path, digest in manifest['files'].items():
        require(not Path(path).is_absolute() and '..' not in Path(path).parts, 'Bad artifact path')
        require(sha(STAGE/path) == digest, 'Artifact drift: '+path)
    candidate = s.inspect(IMAGE)
    require(candidate['Id'] == IMAGE and candidate['Architecture'] == 'amd64', 'Wrong API image')
    require(candidate['Config']['Labels']['org.opencontainers.image.revision'] == APP_COMMIT, 'Wrong source revision')


def unchanged_containers(before, after):
    for name in s.NAMES:
        if name != 'iot_api':
            require(after[name] == before[name], 'Unrelated container changed: '+name)


def guard():
    out(['python3', '/usr/local/libexec/upat-pg-host-preflight.py', '/etc/upat-nzc/postgres-volume.json'])
    identity = s.psql("BEGIN READ ONLY; SELECT system_identifier::text||'|'||pg_postmaster_start_time()::text FROM pg_control_system(); COMMIT;")
    require(identity == '7618955918777626661|'+s.POSTMASTER, 'PostgreSQL identity/start changed')
    require(s.inspect('iot_postgres')['Id'] == s.PG_ID, 'PostgreSQL replaced')
    require(out(['systemctl','is-active','upat-postgres-volume.service']).strip() == 'active', 'PG unit inactive')
    before_path = RECEIPTS/'before.json'
    before = json.loads((before_path if before_path.exists() else PREVIOUS/'before.json').read_text())
    baseline = before['containers']
    if not before_path.exists():
        baseline['shelly_ingestor'] = json.loads((PREVIOUS/'activated.json').read_text())['candidate']
    unchanged_containers(baseline, s.snapshot())
    writer = s.inspect('shelly_ingestor')
    require('SHELLY_MEASUREMENTS_WRITE_MODE=dual' in writer['Config']['Env'], 'Writer is not dual')
    if before_path.exists():
        require(sha(ROOT/'.env') == before['env_sha'], 'Environment drift')


def live_state():
    raw = s.psql("""BEGIN READ ONLY; SET LOCAL statement_timeout='10s';
SELECT json_build_object('at',now(),'postmaster_start',pg_postmaster_start_time(),
'legacy_id',(SELECT id FROM public.shelly_measurements ORDER BY id DESC LIMIT 1),
'compact_id',(SELECT id FROM shelly_compact.measurements ORDER BY id DESC LIMIT 1),
'event_age_seconds',(SELECT extract(epoch FROM now()-event_time) FROM shelly_compact.measurements ORDER BY id DESC LIMIT 1),
'invalid_indexes',(SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='shelly_compact' AND (NOT i.indisvalid OR NOT i.indisready)),
'wal_bytes',(SELECT sum(size) FROM pg_ls_waldir())); COMMIT;""")
    value=json.loads(raw)
    require(value['legacy_id'] == value['compact_id'], 'Live heads differ')
    require(-120 < value['event_age_seconds'] < 180, 'Stale live measurements')
    require(value['invalid_indexes'] == 0, 'Invalid compact index')
    st=os.statvfs(s.VOLUME); value['volume_free_bytes']=st.f_bavail*st.f_frsize
    require(value['volume_free_bytes'] > 20*1024**3, 'Insufficient storage reserve')
    return value


def fetch(base, path, token=None, expected=200):
    headers={'Authorization':'Bearer '+token} if token else {}
    started=time.monotonic()
    try:
        with urlopen(Request(base+path,headers=headers),timeout=70) as r:
            status=r.status; body=json.load(r)
    except HTTPError as e:
        status=e.code; body=json.load(e)
    require(status == expected, f'Unexpected HTTP {status} for {path.split("?")[0]}')
    return body,round(time.monotonic()-started,4)


def ready(base):
    for _ in range(120):
        try:
            body,_=fetch(base,'/health')
            require(body == {'status':'ok','database':'connected'}, 'Unhealthy API')
            return
        except (URLError,TimeoutError,ConnectionError): time.sleep(.5)
    raise RuntimeError('API readiness deadline exceeded')


def auth_probes(base):
    env=dict(x.split('=',1) for x in s.inspect('iot_api')['Config']['Env'])
    token=env.get('DATA_SERVICE_TOKEN','')
    require(len(token)>=32,'Missing existing data service authentication')
    fetch(base,'/internal/data/health',expected=401)
    fetch(base,'/internal/data/health',token='invalid-stage2-probe',expected=401)
    body,_=fetch(base,'/internal/data/health',token=token)
    require(body=={'status':'ok','database':'connected'},'Authenticated health failed')
    fetch(base,'/energy/schools/school_3/devices',expected=401)
    return {'service_token_accepted':True,'missing_and_invalid_token_rejected':True,
            'unauthenticated_school_access_rejected':True}


def cases():
    end=dt.datetime.now(dt.timezone.utc).replace(second=0,microsecond=0)-dt.timedelta(minutes=2)
    device='shellypro3em-ac15187c7da8'
    result=[]
    for name,delta,interval,metrics in [
        ('power_15m',dt.timedelta(minutes=15),'1m',['a_act_power']),
        ('all_1h',dt.timedelta(hours=1),'1m',None),
        ('power_24h',dt.timedelta(days=1),'1h',['a_act_power']),
        ('multiple_24h',dt.timedelta(days=1),'15m',['a_act_power','b_act_power']),
        ('power_7d',dt.timedelta(days=7),'1h',['a_act_power']),
        ('calendar_days',dt.timedelta(days=3),'1d',['a_act_power']),
    ]:
        params={'start':(end-delta).isoformat(),'end':end.isoformat(),'interval':interval}
        if metrics:params['metric']=metrics
        result.append({'name':name,'path':f'/shelly/device/{device}/history?'+urlencode(params,doseq=True)})
    for name,start,stop,interval in [
        ('athens_offset','2026-09-15T09:00:00+03:00','2026-09-15T10:00:00+03:00','5m'),
        ('dst_boundary','2026-03-29T00:00:00Z','2026-03-29T04:00:00Z','1h'),
    ]:
        result.append({'name':name,'path':f'/shelly/device/{device}/history?'+urlencode({'start':start,'end':stop,'interval':interval,'metric':'a_act_power'})})
    result.append({'name':'empty_device','path':result[0]['path'].replace(device,'codex-stage2-nonexistent-device')})
    result.append({'name':'empty_metric','path':result[0]['path'].replace('a_act_power','codex_nonexistent_metric')})
    return result


def preflight():
    artifacts();guard();ready('http://127.0.0.1:8000')
    require(sha(CANONICAL)==sha(STAGE/'ops/shelly-compact/compose.stage1.prod.yml'),'Canonical Compose drift')
    require((ROOT/'docker-compose.yml').is_symlink() and (ROOT/'docker-compose.yml').resolve()==CANONICAL,'Default Compose drift')
    before=s.model(CANONICAL); after=s.model(CANDIDATE)
    s.validate_model(after)
    require(after==expected_after(before),'Unapproved resolved Compose difference')
    require(s.inspect('iot_api')['Image']==OLD_IMAGE,'Unexpected active API image')
    verified=json.loads((PREVIOUS/'verify.json').read_text())
    require(verified['status']=='PASS','Missing stage-one full verification')
    require(time.time()-(PREVIOUS/'verify.json').stat().st_mtime < 7200,'Full verification is no longer recent')
    backup=json.loads((PREVIOUS.parent/'BACKUP-GATE.json').read_text())
    require(backup['restore']=='PASS' and backup['chris_copy']=='PASS','Backup gate missing')
    stamp=dt.datetime.fromisoformat(backup['dump']['finished_utc'])
    require((dt.datetime.now(dt.timezone.utc)-stamp).total_seconds()<21600,'Backup is no longer recent')
    lo=int(verified['results']['forward']['max_id'])
    bits=lambda alias:','.join(f'pg_catalog.{fn}({alias}.{col})' for col,fn in [('device_id','textsend'),('metric','textsend'),('value','float8send'),('unit','textsend'),('event_time','timestamptz_send')])
    bad=s.psql(f"""BEGIN READ ONLY; SET LOCAL statement_timeout='30s';
SELECT count(*) FROM (SELECT l.id FROM (SELECT * FROM public.shelly_measurements WHERE id>{lo}) l
FULL JOIN (SELECT * FROM shelly_compact.readings WHERE id>{lo}) c USING(id)
WHERE l.id IS NULL OR c.id IS NULL OR ROW({bits('l')}) IS DISTINCT FROM ROW({bits('c')}) LIMIT 1) d; COMMIT;""")
    require(bad=='0','New measurements differ since full verification')
    value={'status':'PASS','containers':s.snapshot(),'env_sha':sha(ROOT/'.env'),
           'compose_sha':sha(CANONICAL),'candidate_compose_sha':sha(CANDIDATE),
           'full_verify_rows':verified['results']['forward']['rows'],
           'tail_since_id':lo,'tail_mismatches':0,'live':live_state(),
           'created_utc':dt.datetime.now(dt.timezone.utc).isoformat()}
    (RECEIPTS/'compose.before.yml').write_bytes(CANONICAL.read_bytes())
    save('before.json',value)
    return value


def preview(mode):
    name='codex_shelly_stage2_'+mode
    exists=subprocess.run(['docker','container','inspect',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    require(exists.returncode!=0,'Preview container already exists')
    env=dict(x.split('=',1) for x in s.inspect('iot_api')['Config']['Env'])
    env.update(SHELLY_MEASUREMENTS_READ_STORAGE=mode,SHELLY_MEASUREMENTS_ROUNDING='decimal_1',
               PYTHONDONTWRITEBYTECODE='1',NUMBA_CACHE_DIR='/tmp/numba-cache')
    args=['docker','run','-d','--name',name,'--network',s.NETWORK,'--cpus','.75','--memory','384m',
          '--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=32m','--cap-drop','ALL',
          '--security-opt','no-new-privileges:true','-p','127.0.0.1::8000',
          '--mount',f'type=bind,src={STAGE}/ops/shelly-compact/shadow-api.py,dst=/stage-shadow.py,readonly']
    for key in env:args+=['-e',key]
    # Secret values are inherited privately, never included in argv or output.
    subprocess.run(args+['--entrypoint','python',IMAGE,'/stage-shadow.py'],env={**os.environ,**env},
                   check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    return name


def probe():
    artifacts();guard()
    before=json.loads((RECEIPTS/'before.json').read_text())
    require(sha(CANONICAL)==before['compose_sha'],'Compose changed before preview')
    names=[]; bases={}
    try:
        for mode in ('legacy','compact'):
            name=preview(mode); names.append(name)
            port=s.inspect(name)['NetworkSettings']['Ports']['8000/tcp'][0]['HostPort']
            bases[mode]='http://127.0.0.1:'+port; ready(bases[mode])
        items=[]
        for case in cases():
            old,t1=fetch(bases['legacy'],case['path']);new,t2=fetch(bases['compact'],case['path'])
            require(old==new,'API equality failed: '+case['name'])
            if case['name'].startswith('empty_'):require(new['count']==0,'Empty result failed')
            elif case['name']!='dst_boundary':require(new['count']>0,'Unexpected empty history')
            items.append({**case,'legacy_seconds':t1,'compact_seconds':t2,'count':new['count'],
                          'sha256':hashlib.sha256(json.dumps(new,sort_keys=True).encode()).hexdigest(),
                          'expected':new})
            print(json.dumps({'probe':case['name'],'status':'PASS','legacy_seconds':t1,'compact_seconds':t2}),flush=True)
            live_state()
        # Latest reads are unbounded. Filter one metric and exclude the open minute
        # when comparing sequential calls, while requiring common closed buckets.
        path='/shelly/device/shellypro3em-ac15187c7da8/latest?metric=a_act_power&limit=4'
        left,t1=fetch(bases['legacy'],path);right,t2=fetch(bases['compact'],path)
        cutoff=dt.datetime.now(dt.timezone.utc).replace(second=0,microsecond=0)-dt.timedelta(minutes=1)
        def closed(body):return {x['event_time']:x for x in body['items'] if dt.datetime.fromisoformat(x['event_time'])<cutoff}
        l,r=closed(left),closed(right); common=set(l)&set(r)
        require(len(common)>=1 and all(l[k]==r[k] for k in common),'Latest closed buckets differ')
        latest={'path':path,'closed_buckets_compared':len(common),'legacy_seconds':t1,'compact_seconds':t2}
        authorization=auth_probes(bases['compact'])
        guard();value={'status':'PASS','history_cases':items,'latest':latest,'auth':authorization,
                       'live':live_state(),'created_utc':dt.datetime.now(dt.timezone.utc).isoformat()}
        save('probe.json',value)
        return {'status':'PASS','history_cases':len(items),'latest':latest,'auth':authorization}
    finally:
        for name in reversed(names):
            subprocess.run(['docker','stop','--time','10',name],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            subprocess.run(['docker','rm',name],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)


def recreate_api():
    subprocess.run(s.compose(CANONICAL,'up','-d','--no-deps','--no-build','--pull','never','api'),
                   check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)


def activate():
    artifacts();guard()
    before=json.loads((RECEIPTS/'before.json').read_text())
    probe_result=json.loads((RECEIPTS/'probe.json').read_text())
    require(probe_result['status']=='PASS','Preview gate failed')
    require(time.time()-(RECEIPTS/'probe.json').stat().st_mtime < 1800,'Preview no longer recent')
    require(s.snapshot()==before['containers'],'Runtime changed before cutover')
    require(sha(CANONICAL)==before['compose_sha'],'Canonical Compose changed')
    require(s.model(CANDIDATE)==expected_after(s.model(CANONICAL)),'Resolved model drift')
    started=time.monotonic()
    try:
        s.atomic_config(CANDIDATE.read_bytes())
        recreate_api();ready('http://127.0.0.1:8000')
        ready_seconds=round(time.monotonic()-started,2)
        env=dict(x.split('=',1) for x in s.inspect('iot_api')['Config']['Env'])
        require(s.inspect('iot_api')['Image']==IMAGE,'Wrong active API image')
        require(env['SHELLY_MEASUREMENTS_READ_STORAGE']=='compact' and env['SHELLY_MEASUREMENTS_ROUNDING']=='decimal_1','Wrong active read settings')
        checks=[]
        for case in probe_result['history_cases']:
            body,seconds=fetch('http://127.0.0.1:8000',case['path'])
            require(body==case['expected'],'Active API differs: '+case['name'])
            checks.append({'name':case['name'],'seconds':seconds,'count':body['count']})
        authorization=auth_probes('http://127.0.0.1:8000')
        guard();live=live_state()
        require(live['compact_id']>before['live']['compact_id'],'Ingestion did not advance')
        require(sha(CANONICAL)==before['candidate_compose_sha'],'Canonical config mismatch')
        ownership=json.loads(out(['python3',str(ROOT/'ops/postgres-volume/check-compose.py'),'--check-default']))
        require(ownership['status']=='PASS','Compose ownership check failed')
        value={'status':'PASS','source_commit':APP_COMMIT,'image':IMAGE,
               'read_storage':'compact','rounding':'decimal_1','write_mode':'dual',
               'ready_seconds':ready_seconds,'checks':checks,'auth':authorization,
               'containers':s.snapshot(),'live':live,'postgres_and_ingestor_unchanged':True,
               'created_utc':dt.datetime.now(dt.timezone.utc).isoformat()}
        save('activated.json',value)
        return value
    except BaseException:
        s.atomic_config((RECEIPTS/'compose.before.yml').read_bytes())
        recreate_api();ready('http://127.0.0.1:8000');guard()
        require(s.inspect('iot_api')['Image']==OLD_IMAGE,'Original API rollback failed')
        save('rollback.json',{'status':'PASS','original_api_restored':True,'containers':s.snapshot()})
        raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase',choices=['preflight','probe','activate','status'])
    a=p.parse_args();require(os.geteuid()==0,'Host runner requires root')
    os.umask(0o077);RECEIPTS.mkdir(mode=0o700,exist_ok=True)
    with open('/run/lock/shelly-compact-stage1.lock','w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if a.phase=='status':
            guard();ready('http://127.0.0.1:8000');result={'status':'PASS','containers':s.snapshot(),'live':live_state()}
        else: result={'preflight':preflight,'probe':probe,'activate':activate}[a.phase]()
        print(json.dumps(result),flush=True)


if __name__=='__main__':main()
