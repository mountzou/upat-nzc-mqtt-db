import copy
import hashlib
import json
import os
import re
import subprocess
import time
from datetime import datetime,timezone
from pathlib import Path

ROOT=Path('/opt/upat-nzc-mqtt-db')
RELEASE=Path('/opt/schoolheroz-compact-only-20261005-0ac942b9')
LIVE=ROOT/'docker-compose.prod.yml'
CANDIDATE=RELEASE/'candidate.yml'
ORIGINAL_SHA='53f1cd6abeeebef6faf8ad01dbc567afffef120b61ebdeb2bafac4c7a28e991d'
CANDIDATE_SHA='86bb4e9c0370429ab7e9b0f82a3e184ccaa1bc26b343d76351d2a99d7fe9ed69'
PACKAGE_SHA='18d9497f772189f83fab6cc4bcfdada94011f667dcd7fe0acc206af71fd3d525'
REFS={'api':'schoolheroz-api:compact-only-overlay-0ac942b9b89e',
      'shelly-ingestor':'schoolheroz-shelly-ingestor:compact-only-overlay-0ac942b9b89e'}
NEW={'/iot_api':'sha256:c1d1970522a08e90b571313146da7ce9b725e3341120ad424f35bdfc25c7218a',
     '/shelly_ingestor':'sha256:6d3e2f29bc682be29fa91cdeebc925408eceed33afe7aeea56fa851a4bf1aac9'}
OLD={'/iot_api':'sha256:ae28e0f1b4e4207455f32bb49ec56a4ac78706ba788181fbc6a9d66bdc4e1a18',
     '/shelly_ingestor':'sha256:f92a082e0fbbb5cbbd4f3ab922b589df1ca22b403d4f291bbd8f30ace3308d7b'}
CONTAINERS=['iot_api','shelly_ingestor','iot_postgres','nzc_energyplus','iot_mosquitto','iot_caddy','ttn_ingestor']

def emit(value):
    print(json.dumps(value,default=str),flush=True)

def run(args,input=None,timeout=90):
    return subprocess.run(args,input=input,check=True,capture_output=True,text=True,timeout=timeout).stdout

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        while block:=f.read(1024*1024):h.update(block)
    return h.hexdigest()

def compose(path,*args):
    return ['docker','compose','--project-directory',str(ROOT),'--env-file',str(ROOT/'.env'),
            '-p','upat-nzc-mqtt-db','-f',str(path),*args]

def inspect():
    values=json.loads(run(['docker','inspect',*CONTAINERS]))
    return {v['Name']:{'id':v['Id'],'image':v['Image'],'started_at':v['State']['StartedAt'],
             'status':v['State']['Status'],'restart_count':v['RestartCount']} for v in values}

def install(data,mode):
    temp=ROOT/'.docker-compose.prod.yml.compact-only.tmp'
    temp.write_bytes(data)
    temp.chmod(mode)
    os.replace(temp,LIVE)

DB_CODE='''import json,database
c=database.get_connection();c.set_session(readonly=True)
try:
    with c.cursor() as q:
        q.execute("SET LOCAL statement_timeout='5s'")
        q.execute("SELECT pg_postmaster_start_time() AS postgres_started_at,to_regclass('public.shelly_measurements') AS legacy_table,to_regclass('public.shelly_measurements_id_seq')::oid AS sequence_oid,to_regclass('shelly_compact.measurements')::oid AS compact_oid")
        result=q.fetchone()
        q.execute("SELECT m.id,s.device_id,s.metric,m.event_time FROM shelly_compact.measurements m JOIN shelly_compact.series s USING(series_id) ORDER BY m.id DESC LIMIT 1")
        result['latest']=q.fetchone()
        q.execute("SELECT id,device_id,metric,event_time FROM shelly_compact.readings WHERE id IN (10938671,86962336) ORDER BY id")
        result['history_samples']=q.fetchall()
        print(json.dumps(result,default=str))
    c.rollback()
finally:c.close()
'''

def database():
    return json.loads(run(['docker','exec','-i','iot_api','python','-B','-'],input=DB_CODE))

def health():
    code='import json,urllib.request; r=urllib.request.urlopen("http://127.0.0.1:8000/health",timeout=3); print(json.dumps({"status":r.status,"body":json.loads(r.read())}))'
    return json.loads(run(['docker','exec','iot_api','python','-B','-c',code],timeout=10))

before=None;original=None;mode=None;installed=False;phase='preflight'
try:
    assert digest(LIVE)==ORIGINAL_SHA and digest(CANDIDATE)==CANDIDATE_SHA
    assert digest(RELEASE/'images.tar.gz')==PACKAGE_SHA
    before=inspect()
    assert all(v['status']=='running' for v in before.values())
    assert all(before[name]['image']==image for name,image in OLD.items())
    assert before['/iot_postgres']['id']=='3e8abad67a5a4bbfdb2dd0773287d808e83b49a294634cff9d4d57af8efc5e1d'
    baseline_db=database()
    assert baseline_db['legacy_table'] is None and baseline_db['sequence_oid']==16445 and baseline_db['compact_oid']==121068
    original=LIVE.read_bytes();mode=LIVE.stat().st_mode&0o777
    rollback=RELEASE/'rollback'/'docker-compose.prod.yml'
    rollback.write_bytes(original);rollback.chmod(0o600)
    (RELEASE/'before.json').write_text(json.dumps({'containers':before,'database':baseline_db,'compose_sha256':ORIGINAL_SHA},indent=2))
    emit({'phase':'baseline_saved','latest_id':baseline_db['latest']['id']})
    phase='image_load'
    run(['docker','image','load','--input',str(RELEASE/'images.tar.gz')],timeout=120)
    for service,ref in REFS.items():
        item=json.loads(run(['docker','image','inspect',ref]))[0]
        container='/iot_api' if service=='api' else '/shelly_ingestor'
        assert item['Id']==NEW[container] and item['Architecture']=='amd64' and item['Os']=='linux'
        assert item['Config']['Labels']['org.opencontainers.image.revision']=='0ac942b9b89e9fc5b46cc8ff440fa9788157315c'
    original_model=json.loads(run(compose(LIVE,'--profile','*','config','--format','json')))
    candidate_model=json.loads(run(compose(CANDIDATE,'--profile','*','config','--format','json')))
    expected=copy.deepcopy(original_model)
    for service,ref in REFS.items():
        expected['services'][service]['image']=ref
        for key in ['SHELLY_MEASUREMENTS_READ_STORAGE','SHELLY_MEASUREMENTS_WRITE_MODE','SHELLY_MEASUREMENTS_ROUNDING']:
            expected['services'][service]['environment'].pop(key,None)
    assert candidate_model==expected
    assert digest(LIVE)==ORIGINAL_SHA and inspect()==before
    emit({'phase':'images_and_targeted_compose_verified','changed_services':list(REFS)})
    phase='activation';activation=datetime.now(timezone.utc).isoformat()
    install(CANDIDATE.read_bytes(),mode);installed=True
    run(compose(LIVE,'up','-d','--no-deps','--no-build','--pull','never','api','shelly-ingestor'),timeout=120)
    emit({'phase':'services_recreated','activation_started_at_utc':activation})
    phase='health_acceptance'
    for _ in range(30):
        try:
            current_health=health()
            if current_health=={'status':200,'body':{'status':'ok','database':'connected'}}:break
        except Exception:pass
        time.sleep(1)
    else:raise RuntimeError('API health acceptance failed')
    phase='ingestion_acceptance'
    for _ in range(45):
        post_db=database()
        if post_db['latest']['id']>baseline_db['latest']['id']:break
        time.sleep(1)
    else:raise RuntimeError('No new compact measurements observed')
    assert post_db['legacy_table'] is None and post_db['sequence_oid']==16445 and post_db['compact_oid']==121068
    assert post_db['postgres_started_at']==baseline_db['postgres_started_at']
    assert [x['id'] for x in post_db['history_samples']]==[10938671,86962336]
    after=inspect()
    for name,state in after.items():
        if name in NEW:
            assert state['image']==NEW[name] and state['id']!=before[name]['id'] and state['status']=='running' and state['restart_count']==0
        else:assert state==before[name]
    phase='final_validator'
    validator=json.loads(run(['python3',str(ROOT/'ops/postgres-volume/check-compose.py'),'--check-default']))
    assert validator['status']=='PASS'
    selectors='import os,json; print(json.dumps([k for k in os.environ if k.startswith("SHELLY_MEASUREMENTS_")]))'
    for container in ['iot_api','shelly_ingestor']:
        assert json.loads(run(['docker','exec',container,'python','-B','-c',selectors]))==[]
    receipt={'status':'PASS','activated_at_utc':activation,'verified_at_utc':datetime.now(timezone.utc).isoformat(),
             'before':before,'after':after,'database_before':baseline_db,'database_after':post_db,
             'health':current_health,'validator':validator,'compose_sha256':digest(LIVE),
             'scope':['api','shelly-ingestor'],'postgresql_and_other_services_unchanged':True}
    (RELEASE/'activated.json').write_text(json.dumps(receipt,indent=2))
    emit({'status':'PASS','receipt':str(RELEASE/'activated.json'),'before_latest_id':baseline_db['latest']['id'],
          'after_latest_id':post_db['latest']['id'],'other_services_unchanged':True})
except Exception as error:
    emit({'status':'ERROR','phase':phase,'error_type':type(error).__name__})
    if installed:
        try:
            install(original,mode)
            run(compose(LIVE,'up','-d','--no-deps','--no-build','--pull','never','api','shelly-ingestor'),timeout=120)
            emit({'status':'ROLLED_BACK','restored_compose_sha256':digest(LIVE)})
        except Exception as rollback_error:
            emit({'status':'ROLLBACK_ERROR','error_type':type(rollback_error).__name__})
    raise SystemExit(1)
