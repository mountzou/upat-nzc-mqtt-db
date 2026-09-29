#!/usr/bin/env python3
"""Approved writer-only cutover with certified, resumable tail rollback."""
import argparse
import copy
import datetime as dt
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time

STAGE=Path(__file__).resolve().parents[2]
def module(name):
    spec=importlib.util.spec_from_file_location(name,STAGE/'ops/shelly-compact'/f'{name}.py')
    obj=importlib.util.module_from_spec(spec);spec.loader.exec_module(obj);return obj
s=module('stage1');api=module('stage2')
ROOT=s.ROOT;RECEIPTS=STAGE/'receipts';IMAGE=s.IMAGE
CANONICAL=ROOT/'docker-compose.prod.yml'
CANDIDATE=STAGE/'ops/shelly-compact/compose.stage3.prod.yml'
require,out,sha=s.require,s.out,s.sha
s.RECEIPTS=RECEIPTS


def save(name,value):
    with (RECEIPTS/name).open('x') as f:
        json.dump(value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())


def artifacts():
    manifest=json.loads((STAGE/'ARTIFACT-MANIFEST.json').read_text())
    for path,digest in manifest['files'].items():
        require(not Path(path).is_absolute() and '..' not in Path(path).parts,'Bad artifact path')
        require(sha(STAGE/path)==digest,'Artifact drift: '+path)
    image=s.inspect(IMAGE)
    require(image['Id']==IMAGE and image['Architecture']=='amd64','Wrong writer image')
    require(image['Config']['Labels']['org.opencontainers.image.revision']==s.APP_COMMIT,'Wrong app source')


def guard():
    s.db_guard()
    require(s.inspect('iot_api')['Id']=='bbc1dbcb5c856d7e3014dc5d8c24d33ce8a8e72afabbf0f26abe34deb18ff1a0','API changed')
    require('SHELLY_MEASUREMENTS_READ_STORAGE=compact' in s.inspect('iot_api')['Config']['Env'],'API not compact')
    if (RECEIPTS/'before.json').exists():
        require(sha(ROOT/'.env')==json.loads((RECEIPTS/'before.json').read_text())['env_sha'],'Environment drift')
    st=os.statvfs(s.VOLUME)
    require(st.f_bavail*st.f_frsize>20*1024**3,'Insufficient volume reserve')


def state():
    return json.loads(s.psql("""BEGIN READ ONLY; SET LOCAL statement_timeout='10s';
SELECT json_build_object('at',now(),'postmaster_start',pg_postmaster_start_time(),
'legacy_id',(SELECT id FROM public.shelly_measurements ORDER BY id DESC LIMIT 1),
'compact_id',(SELECT id FROM shelly_compact.measurements ORDER BY id DESC LIMIT 1),
'event_age_seconds',(SELECT extract(epoch FROM now()-event_time) FROM shelly_compact.measurements ORDER BY id DESC LIMIT 1),
'invalid_indexes',(SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='shelly_compact' AND (NOT i.indisvalid OR NOT i.indisready)),
'wal_bytes',(SELECT sum(size) FROM pg_ls_waldir())); COMMIT;"""))


def expected(before):
    result=copy.deepcopy(before)
    result['services']['shelly-ingestor']['environment']['SHELLY_MEASUREMENTS_WRITE_MODE']='compact'
    return result


def worker(phase,*args):
    values=dict(x.split('=',1) for x in s.inspect('shelly_ingestor')['Config']['Env'])
    db={key:values[source] for key,source in [('host','POSTGRES_HOST'),('port','POSTGRES_INTERNAL_PORT'),('dbname','POSTGRES_DB'),('user','POSTGRES_USER'),('password','POSTGRES_PASSWORD')]}
    require(db['host']=='postgres' and db['dbname']=='iot_db' and db['user']=='postgres','Wrong database destination')
    quote=lambda v:"'"+v.replace('\\','\\\\').replace("'","\\'")+"'"
    env={**os.environ,'SHELLY_MIGRATION_DSN':' '.join(k+'='+quote(v) for k,v in db.items()),
         'PGOPTIONS':'-c work_mem=32MB -c temp_file_limit=12GB -c max_parallel_workers_per_gather=0',
         'PYTHONDONTWRITEBYTECODE':'1'}
    command=['docker','run','--rm','--name','shelly_compact_stage3_job','--network',s.NETWORK,
             '--cpus','.75','--memory','512m','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges:true',
             '--mount',f'type=bind,src={STAGE},dst=/stage,readonly',
             '--mount',f'type=bind,src={RECEIPTS},dst=/receipts',
             '--mount',f'type=bind,src={s.VOLUME},dst=/space-check,readonly']
    for key in ['SHELLY_MIGRATION_DSN','PGOPTIONS','PYTHONDONTWRITEBYTECODE']: command+=['-e',key]
    subprocess.run(command+['--entrypoint','python',IMAGE,'/stage/ops/shelly-compact/writer-cutover.py',phase,*args],env=env,check=True)


def preflight():
    artifacts();guard()
    require(sha(CANONICAL)==sha(STAGE/'ops/shelly-compact/compose.stage2.prod.yml'),'Canonical Compose drift')
    require((ROOT/'docker-compose.yml').is_symlink() and (ROOT/'docker-compose.yml').resolve()==CANONICAL,'Default Compose drift')
    before=s.model(CANONICAL);after=s.model(CANDIDATE);s.validate_model(after)
    require(after==expected(before),'Unapproved Compose differences')
    writer=s.inspect('shelly_ingestor')
    require(writer['Id']=='16b01f0f241b67bc354ded03d970188064230d73678ad9dea462e099e6f57325' and writer['Image']==IMAGE,'Writer drift')
    require('SHELLY_MEASUREMENTS_WRITE_MODE=dual' in writer['Config']['Env'],'Writer not dual')
    require(s.psql("SELECT count(*) FROM shelly_compact.copy_progress WHERE direction='reverse'")=='0','Unexpected prior reverse checkpoint')
    live=state();require(live['legacy_id']==live['compact_id'],'Dual heads differ')
    value={'status':'PASS','created_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
           'containers':s.snapshot(),'env_sha':sha(ROOT/'.env'),'compose_sha':sha(CANONICAL),'live':live}
    (RECEIPTS/'compose.before.yml').write_bytes(CANONICAL.read_bytes())
    save('before.json',value);return value


def recreate():
    subprocess.run(s.compose(CANONICAL,'up','-d','--no-deps','--no-build','--pull','never','--force-recreate','shelly-ingestor'),check=True)


def stop():
    subprocess.run(['docker','stop','--timeout','30','shelly_ingestor'],check=True,stdout=subprocess.DEVNULL)
    require(not s.inspect('shelly_ingestor')['State']['Running'],'Writer still running')


def rollback(reason):
    # Also usable after a failed process: DB checkpoint presence is authoritative.
    guard();stop()
    row=s.psql("SELECT high_water FROM shelly_compact.copy_progress WHERE direction='reverse'")
    recovered=None
    if row:
        cp=RECEIPTS/'checkpoint.json'
        # Before first activation, reverse.high_water equals certified baseline.
        baseline=json.loads(cp.read_text())['high_water'] if cp.exists() else int(row)
        name='recovered-'+str(time.time_ns())+'.json'
        worker('recover','--writers-paused','--baseline',str(baseline),'--receipt','/receipts/'+name)
        recovered=json.loads((RECEIPTS/name).read_text())
    require(sha(ROOT/'.env')==json.loads((RECEIPTS/'before.json').read_text())['env_sha'],'Environment drift')
    s.atomic_config((RECEIPTS/'compose.before.yml').read_bytes());recreate()
    for _ in range(60):
        live=state()
        if live['legacy_id']==live['compact_id'] and live['event_age_seconds']<60:break
        time.sleep(1)
    require(live['legacy_id']==live['compact_id'] and live['event_age_seconds']<60,'Dual rollback not healthy')
    guard()
    value={'status':'PASS','reason':reason,'recovery':recovered,'live':live,'writer':s.snapshot()['shelly_ingestor']}
    save('rollback-'+str(time.time_ns())+'.json',value);return value


def status():
    guard()
    before=json.loads((RECEIPTS/'before.json').read_text())
    high=json.loads((RECEIPTS/'checkpoint.json').read_text())['high_water']
    writer=s.inspect('shelly_ingestor')
    require(writer['State']['Running'] and writer['RestartCount']==0,'Writer unhealthy')
    require(writer['Image']==IMAGE and 'SHELLY_MEASUREMENTS_WRITE_MODE=compact' in writer['Config']['Env'],'Writer not compact')
    require(sha(CANONICAL)==sha(CANDIDATE),'Canonical Compose drift')
    live=state()
    require(live['legacy_id']==high and live['compact_id']>high,'Legacy not frozen or compact not advancing')
    require(-120<live['event_age_seconds']<120 and live['invalid_indexes']==0,'Compact unhealthy')
    require(live['wal_bytes']<4*1024**3,'WAL budget exceeded')
    return {'status':'PASS','mode':'compact','checkpoint':high,'live':live,'containers':s.snapshot(),
            'new_rows':int(s.psql(f'SELECT count(*) FROM shelly_compact.measurements WHERE id>{high}')),
            'postgres_and_api_unchanged':True}


def activate():
    artifacts();guard()
    before=json.loads((RECEIPTS/'before.json').read_text())
    require(sha(CANONICAL)==before['compose_sha'],'Compose changed since preflight')
    require(s.snapshot()['shelly_ingestor']==before['containers']['shelly_ingestor'],'Writer changed since preflight')
    require(s.model(CANDIDATE)==expected(s.model(CANONICAL)),'Candidate changed')
    gate=json.loads((STAGE/'BACKUP-GATE.json').read_text())
    require(gate['restore']=='PASS' and gate['chris_copy']=='PASS','Missing verified backup copies')
    stamp=dt.datetime.fromisoformat(gate['dump']['finished_utc'])
    require(0<(dt.datetime.now(dt.timezone.utc)-stamp).total_seconds()<21600,'Backup is stale')
    proof=json.loads((RECEIPTS/'verify.json').read_text())
    require(proof['status']=='PASS','Full verification failed')
    started=time.monotonic()
    try:
        stop()
        worker('checkpoint','--writers-paused','--proof','/receipts/verify.json','--receipt','/receipts/checkpoint.json')
        s.atomic_config(CANDIDATE.read_bytes());recreate()
        recreated=time.monotonic()
        high=json.loads((RECEIPTS/'checkpoint.json').read_text())['high_water']
        for _ in range(90):
            if state()['compact_id']>high: break
            time.sleep(1)
        value=status()
        value.update(app_commit=s.APP_COMMIT,image=IMAGE,stop_to_recreate_seconds=round(recreated-started,2),finished_utc=dt.datetime.now(dt.timezone.utc).isoformat())
        save('activated.json',value);return value
    except Exception as e:
        rollback(type(e).__name__)
        raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase',choices=['preflight','verify','activate','status','rollback'])
    a=p.parse_args();os.umask(0o077);RECEIPTS.mkdir(mode=0o700,exist_ok=True)
    with open('/run/lock/shelly-compact-stage1.lock','a') as f:
        fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if a.phase=='verify':
            artifacts();guard();worker('verify','--receipt','/receipts/verify.json');result={'status':'PASS','phase':'verify'}
        elif a.phase=='rollback': artifacts();result=rollback('explicit operator request')
        else:result=globals()[a.phase]()
        print(json.dumps(result),flush=True)


if __name__=='__main__': main()
