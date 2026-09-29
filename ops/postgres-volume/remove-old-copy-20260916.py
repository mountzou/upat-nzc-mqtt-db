#!/usr/bin/env python3
"""One-time, explicitly approved removal of the stopped pre-cutover copy only."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

OLD_ID='c7b333bccb47d319b87f620281ccd4a467dda858173fffe87a7577695ca0525d'
OLD_NAME='/iot_postgres-rootdisk-rollback-20260916'
OLD_VOLUME='upat-nzc-mqtt-db_postgres_data'
OLD_PATH='/var/lib/docker/volumes/'+OLD_VOLUME+'/_data'
ACTIVE_ID='3e8abad67a5a4bbfdb2dd0773287d808e83b49a294634cff9d4d57af8efc5e1d'
ACTIVE_PATH='/mnt/HC_Volume_106884142/pgdata'
EVIDENCE=Path('/root/codex-pg-old-copy-removal-20260916')


def require(ok, reason):
    if not ok:raise RuntimeError(reason)


def run(args):
    return subprocess.check_output(args,text=True,stderr=subprocess.PIPE,timeout=120).strip()


def containers():
    ids=run(['docker','ps','-aq']).split()
    return json.loads(run(['docker','inspect',*ids]))


def references_old(c):
    return any(m.get('Name')==OLD_VOLUME or m.get('Source','').rstrip('/')==OLD_PATH or
               m.get('Source','').startswith(OLD_PATH+'/') or
               (m.get('Source') and OLD_PATH.startswith(m['Source'].rstrip('/')+'/')) for m in c['Mounts'])


def validate(cs,volume):
    old=next(c for c in cs if c['Id']==OLD_ID)
    active=next(c for c in cs if c['Id']==ACTIVE_ID)
    require(old['Name']==OLD_NAME,'old container name mismatch')
    require(old['State']['Status']=='exited' and not old['State']['Running'] and old['State']['ExitCode']==0,'old container not cleanly stopped')
    require(old['HostConfig']['RestartPolicy']['Name']=='no','old container can restart automatically')
    require(not old['NetworkSettings']['Networks'],'old container still connected')
    require(active['Name']=='/iot_postgres' and active['State']['Running'],'active DB identity/state mismatch')
    data=[m for m in active['Mounts'] if m['Destination']=='/var/lib/postgresql/data']
    require(len(data)==1 and data[0]['Type']=='bind' and data[0]['Source']==ACTIVE_PATH,'active PGDATA mismatch')
    require([c['Id'] for c in cs if references_old(c)]==[OLD_ID],'old storage has another consumer')
    olddata=[m for m in old['Mounts'] if m['Destination']=='/var/lib/postgresql/data']
    require(len(olddata)==1 and olddata[0]['Name']==OLD_VOLUME and olddata[0]['Source']==OLD_PATH,'old PGDATA mismatch')
    require(volume['Name']==OLD_VOLUME and volume['Mountpoint']==OLD_PATH and volume['Driver']=='local' and not volume.get('Options'),'unexpected volume definition')


def stable(cs):
    return {c['Id']:{'name':c['Name'],'image':c['Image'],'state':c['State']['Status'],
           'started':c['State']['StartedAt'],'restarts':c['RestartCount'],
           'mounts':sorted(c['Mounts'],key=lambda m:m['Destination']),
           'networks':c['NetworkSettings']['Networks']} for c in cs}


def host_refs():
    matches=[]
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():continue
        try:
            for f in (p/'fd').iterdir():
                try:target=os.readlink(f)
                except FileNotFoundError:continue
                if target==OLD_PATH or target.startswith(OLD_PATH+'/'):matches.append(int(p.name))
            if OLD_PATH in (p/'maps').read_text():matches.append(int(p.name))
        except (FileNotFoundError,ProcessLookupError):continue
    require(not matches,'host processes still reference old PGDATA')


def db_check():
    sql="BEGIN READ ONLY; SET LOCAL statement_timeout='3s'; SELECT json_build_object('at',now(),'postgres_start',pg_postmaster_start_time(),'shelly',(SELECT row_to_json(x) FROM (SELECT id,event_time FROM shelly_measurements ORDER BY id DESC LIMIT 1)x),'upat',(SELECT row_to_json(x) FROM (SELECT id,event_time FROM upat_measurements ORDER BY id DESC LIMIT 1)x)); COMMIT;"
    return json.loads(run(['docker','exec','iot_postgres','psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','iot_db','-c',sql]))


def save(name,data):
    with (EVIDENCE/name).open('x') as f:
        json.dump(data,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())


def main():
    require(sys.argv[1:]==['--remove-approved-old-copy'],'explicit removal flag required')
    os.umask(0o077)
    EVIDENCE.mkdir(mode=0o700)  # Refuse accidental reruns.
    started=datetime.datetime.now(datetime.timezone.utc).isoformat()
    require(json.loads(run(['python3','/opt/upat-nzc-mqtt-db/ops/postgres-volume/check-compose.py','--check-default']))['status']=='PASS','Compose preflight failed')
    before=containers();volume=json.loads(run(['docker','volume','inspect',OLD_VOLUME]))[0]
    validate(before,volume)
    require(not Path(OLD_PATH).is_symlink() and Path(OLD_PATH).resolve()==Path(OLD_PATH),'old volume path redirected')
    require(os.stat(OLD_PATH).st_dev==os.stat('/').st_dev and os.stat(OLD_PATH).st_dev!=os.stat(ACTIVE_PATH).st_dev,'unexpected storage devices')
    host_refs()
    images=sorted(set(run(['docker','image','ls','-q','--no-trunc']).split()))
    volumes=sorted(run(['docker','volume','ls','-q']).split())
    db_before=db_check()
    df_before=run(['df','-B1','/','/mnt/HC_Volume_106884142'])
    old_bytes=int(run(['du','-s','-B1',OLD_PATH]).split()[0])
    save('BEFORE.json',{'at':started,'containers':stable(before),'volume':volume,'images':images,'volumes':volumes,'db':db_before,'df':df_before,'old_allocated_bytes':old_bytes})
    # Recheck immediately before mutation. No force, no -v, no Compose down/prune.
    validate(containers(),json.loads(run(['docker','volume','inspect',OLD_VOLUME]))[0])
    require(run(['docker','rm',OLD_ID])==OLD_ID,'unexpected container removal result')
    save('CONTAINER-REMOVED.json',{'container_id':OLD_ID,'container_name':OLD_NAME})
    remaining=containers()
    require(all(not references_old(c) for c in remaining),'old volume still referenced after container removal')
    require(stable(remaining)=={k:v for k,v in stable(before).items() if k!=OLD_ID},'unrelated container changed')
    current=json.loads(run(['docker','volume','inspect',OLD_VOLUME]))[0]
    require(current==volume,'old volume identity changed')
    host_refs()
    require(run(['docker','volume','rm',OLD_VOLUME])==OLD_VOLUME,'unexpected volume removal result')
    save('VOLUME-REMOVED.json',{'volume':OLD_VOLUME})
    after=containers()
    require(stable(after)=={k:v for k,v in stable(before).items() if k!=OLD_ID},'unrelated container changed after removal')
    require(sorted(run(['docker','volume','ls','-q']).split())==[v for v in volumes if v!=OLD_VOLUME],'unexpected volume change')
    require(sorted(set(run(['docker','image','ls','-q','--no-trunc']).split()))==images,'image inventory changed')
    require(not Path(OLD_PATH).exists(),'old PGDATA path still exists')
    db_after=db_check()
    require(db_after['postgres_start']==db_before['postgres_start'],'active PostgreSQL restarted')
    api=json.loads(run(['curl','-fsS','--max-time','10','http://127.0.0.1:8000/health']))
    require(api.get('database')=='connected','API database disconnected')
    finished=datetime.datetime.now(datetime.timezone.utc).isoformat()
    events=[json.loads(line) for line in run(['docker','events','--since',started,'--until',finished,'--format','{{json .}}']).splitlines() if line]
    lifecycle=[{'type':e.get('Type'),'action':e.get('Action'),'id':e.get('Actor',{}).get('ID')} for e in events if e.get('Type')=='container' and e.get('Action') in ('start','stop','restart','kill','die','destroy','create')]
    require(all(e['id']==OLD_ID and e['action']=='destroy' for e in lifecycle),'unexpected container lifecycle event')
    result={'status':'PASS','finished_at':finished,'removed_container':OLD_NAME,'removed_id':OLD_ID,'removed_volume':OLD_VOLUME,'old_allocated_bytes':old_bytes,'df_before':df_before,'df_after':run(['df','-B1','/','/mnt/HC_Volume_106884142']),'database_before':db_before,'database_after':db_after,'api':api,'other_containers_volumes_images_unchanged':True,'lifecycle_events':lifecycle}
    save('REMOVAL-VERIFIED.json',result)
    print(json.dumps(result,indent=2))


if __name__=='__main__':
    try:main()
    except Exception as e:
        print(json.dumps({'status':'FAIL','error_type':type(e).__name__,'reason':str(e) if isinstance(e,RuntimeError) else 'See private operation receipts'}))
        sys.exit(1)
