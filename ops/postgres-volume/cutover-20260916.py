#!/usr/bin/env python3
"""Operator-triggered, single-phase production cutover; never automatically deletes data.
Run on the audited VPS after explicit migration authorization and verified off-host backup.
Private state lives in /root; do not publish its runtime payloads or secrets.
"""
import datetime, hashlib, http.client, importlib.util, json, os, pathlib, shutil, socket, subprocess, sys, time
os.umask(0o077)
R=pathlib.Path('/root/codex-pg-volume-cutover-20260916')
SRC='upat-nzc-mqtt-db_postgres_data'
OLD='iot_postgres-rootdisk-rollback-20260916'
TOOLS='sha256:5436b7ee304b830f48edb48b9bd5b4885610a28f689996497cc42afb322f1899'
NETWORK='upat-nzc-mqtt-db_default'
TIMERS=['upat-iaq-notifications-hourly.timer','upat-iaq-notifications-daily.timer','upat-pv-ingestor.timer']
SERVICES=['upat-iaq-notifications@hourly.service','upat-iaq-notifications@daily.service','upat-pv-ingestor.service']
WRITERS=['iot_api','shelly_ingestor','ttn_ingestor']
def require(ok,msg):
    if not ok: raise RuntimeError(msg)
def run(*a): return subprocess.check_output(a,text=True).strip()
def inspect(n): return json.loads(run('docker','inspect',n))[0]
def save(name,data):
    p=R/name;tmp=p.with_name(p.name+'.tmp')
    with tmp.open('w') as f: json.dump(data,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    tmp.replace(p)
def event(stage,**kw):
    row={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'stage':stage,**kw}
    with (R/'events.jsonl').open('a') as f: f.write(json.dumps(row)+'\n');f.flush();os.fsync(f.fileno())
    print(json.dumps(row),flush=True)
def load(n): return json.loads((R/n).read_text())
def module(name,file):
    s=importlib.util.spec_from_file_location(name,R/file);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
def sql(q):
    return run('docker','exec','-e','PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=300000 -c lock_timeout=1000 -c max_parallel_workers_per_gather=0','iot_postgres','psql','-X','-qAt','-U','postgres','-d','iot_db','-v','ON_ERROR_STOP=1','-c',q)
def qi(n):return '"'+n.replace('"','""')+'"'
def mountcheck():
    s=load('production-spec.json');m=pathlib.Path(s['mountpoint'])
    require(m.is_mount() and not m.is_symlink(),'Volume not mounted')
    f=json.loads(run('findmnt','-J','--mountpoint',str(m),'-o','TARGET,SOURCE,FSTYPE,UUID'))['filesystems'][0]
    require(f['fstype']=='ext4' and f['uuid']==s['filesystem_uuid'],'Wrong filesystem UUID/type')
    require(os.path.samefile(f['source'],s['device']),'Wrong mounted device')
    require(run('lsblk','-dn','-o','SERIAL',s['device'])==s['volume_id'],'Wrong Volume serial')
    require(run('stat','-f','-c','%i',str(m))==s['filesystem_id'],'Wrong filesystem ID')
    return s

def preflight():
    s=mountcheck();pg=inspect('iot_postgres');require(pg['Id']==s['source_container_id'] and pg['Image']==s['image'],'Source identity drift')
    require(pg['State']['Running'] and pg['State']['Health']['Status']=='healthy','Source unhealthy')
    require(sql('SELECT system_identifier FROM pg_control_system();')==s['system_id'],'Wrong source cluster')
    old=next(c for c in load('active-containers-private.json') if c['Name']=='/iot_postgres')
    for c in [old,pg]:
        for k in ['Dns','DnsOptions','DnsSearch']:c['HostConfig'][k]=c['HostConfig'].get(k) or []
    require(all(pg[k]==old[k] for k in ['Id','Image','Config','HostConfig']),'Source runtime drift')
    require(sorted(p.name for p in pathlib.Path(s['mountpoint']).iterdir())==['lost+found'],'Destination not empty')
    require(inspect_image(TOOLS)==TOOLS,'Copy helper image differs')
    require(shutil.disk_usage(s['mountpoint']).free>30*1024**3,'Insufficient destination capacity')
    require(shutil.disk_usage('/').free>4*1024**3,'Insufficient root reserve')
    require(not (R/'state-before.json').exists(),'Maintenance already initialized')
    for p in ['/usr/local/libexec/pg-volume-guard.sh','/usr/local/libexec/upat-pg-host-preflight.py','/etc/upat-nzc/postgres-volume.json','/etc/systemd/system/upat-postgres-volume.service']:
        require(not pathlib.Path(p).exists(),'Refusing to overwrite '+p)
    run('systemd-analyze','verify',str(R/'upat-postgres-volume.service'),'/run/systemd/generator/mnt-HC_Volume_106884142.mount')
    event('preflight_passed')
def inspect_image(n): return json.loads(run('docker','image','inspect',n))[0]['Id']

def active_job_processes():
    markers=['run-energy-aggregator','upat_incremental_rollups','upat_retention_batch','raw-cleanup','weather-collector','pv-prediction','simulation-recorder','upat-pv-ingestor','iaq-notifications']
    found=[]
    for p in pathlib.Path('/proc').glob('[0-9]*'):
        try:
            if int(p.name)==os.getpid():continue
            args=(p/'cmdline').read_bytes().replace(b'\x00',b' ').decode(errors='replace')
            if any(m in args for m in markers):found.append(int(p.name))
        except (FileNotFoundError,ProcessLookupError,PermissionError):pass
    return found

def pause():
    preflight()
    require(load('RESTORE-VERIFIED.json')['status']=='PASS' and load('COPY-VERIFIED.json')['status']=='PASS','Fresh restored backup not verified on Mac/Chris')
    cron=pathlib.Path('/var/spool/cron/crontabs/root').read_bytes()
    audit=load('jobs-audit-sanitized.json');entry=next(x for x in audit['cron'] if x['path']=='/var/spool/cron/crontabs/root')
    require(hashlib.sha256(cron).hexdigest()==entry['sha256'],'Cron changed since audit')
    active=[l for l in cron.decode().splitlines() if l.strip() and not l.lstrip().startswith('#')]
    require(len(active)==7 and all(x['db_related'] for x in entry['entries']),'Unknown scheduled jobs')
    cs=json.loads(run('docker','inspect',*run('docker','ps','-aq').split()))
    require({c['Name'] for c in cs if c['State']['Running']}=={'/iot_postgres','/iot_api','/iot_caddy','/iot_mosquitto','/shelly_ingestor','/ttn_ingestor'},'Unexpected running containers')
    timers={t:{'active':run('systemctl','show',t,'-p','ActiveState','--value'),'enabled':run('systemctl','show',t,'-p','UnitFileState','--value')} for t in TIMERS}
    require(all(v=={'active':'active','enabled':'enabled'} for v in timers.values()),'Unexpected timer state')
    state={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'timers':timers,'cron_original_sha256':hashlib.sha256(cron).hexdigest(),'containers':[{'name':c['Name'],'id':c['Id'],'image':c['Image'],'running':c['State']['Running'],'start':c['State']['StartedAt'],'restart_count':c['RestartCount']} for c in cs]}
    (R/'root-crontab.original').write_bytes(cron);save('state-before.json',state)
    run('systemctl','disable','--now',*TIMERS)
    paused=''.join('# CODEX_VOLUME_PAUSED '+l if l.strip() and not l.lstrip().startswith('#') else l for l in cron.decode().splitlines(keepends=True)).encode()
    subprocess.run(['crontab','-'],input=paused,check=True)
    # Store exactly the listing bytes; crontab rewrites its internal header.
    installed=subprocess.check_output(['crontab','-l']);(R/'root-crontab.paused-listing').write_bytes(installed)
    require(not [l for l in installed.decode().splitlines() if l.strip() and not l.lstrip().startswith('#')],'Cron not paused')
    event('schedules_paused')
    # Let already running scheduled jobs finish without killing them.
    deadline=time.monotonic()+180
    while any(run('systemctl','show',s,'-p','ActiveState','--value') not in ['inactive','failed'] for s in SERVICES):
        require(time.monotonic()<deadline,'Scheduled job did not finish; investigate without forced kill');time.sleep(2)
    deadline=time.monotonic()+180
    while active_job_processes():
        require(time.monotonic()<deadline,'Scheduled job process still running; no forced kill performed');time.sleep(2)
    for n in WRITERS:
        run('docker','stop','--signal','SIGTERM' if n=='iot_api' else 'SIGINT','--timeout','-1',n)
        require(not inspect(n)['State']['Running'],'Writer still running');event('writer_stopped',name=n)
    deadline=time.monotonic()+180
    while int(sql("SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid();")):
        require(time.monotonic()<deadline,'DB clients did not drain');time.sleep(2)
    require(sql('SELECT count(*) FROM pg_prepared_xacts;')=='0','Prepared transactions remain')
    save('PAUSED.json',state);event('writers_quiescent')

def checkpoint(phase):
    require(phase in ['before','after'],'Bad checkpoint phase')
    require((R/'PAUSED.json').exists() and not (R/'RESUMED.json').exists(),'Writers not paused')
    require(all(not inspect(n)['State']['Running'] for n in WRITERS),'Writer running')
    require(not active_job_processes(),'Scheduled job process remains')
    require(sql("SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid();")=='0','Unexpected DB clients')
    tables=json.loads(sql("SELECT json_agg(relname ORDER BY relname) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r';"))
    counts={t:int(sql('SELECT count(*) FROM public.'+qi(t)+';')) for t in tables}
    seqnames=json.loads(sql("SELECT json_agg(relname ORDER BY relname) FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='S';"))
    sequences={n:json.loads(sql('SELECT row_to_json(x) FROM (SELECT last_value,is_called FROM public.'+qi(n)+') x;')) for n in seqnames}
    catalog=json.loads(sql((R/'catalog.sql').read_text()))
    stable_catalog={k:catalog[k] for k in ['indexes','constraints','extensions','settings']}
    stable_catalog['objects']=sorted([x['schema'],x['name'],x['relkind']] for x in catalog['tables'])
    fresh={t:json.loads(sql('SELECT row_to_json(x) FROM (SELECT id,event_time FROM public.'+qi(t)+' ORDER BY id DESC LIMIT 1) x;')) for t in ['shelly_measurements','upat_measurements']}
    data={'counts':counts,'sequences':sequences,'catalog':stable_catalog,'freshness':fresh,'cluster':sql('SELECT system_identifier FROM pg_control_system();'),'database_oid':sql("SELECT oid FROM pg_database WHERE datname='iot_db';")}
    require(data['cluster']==load('production-spec.json')['system_id'] and data['database_oid']=='16384','DB identity mismatch')
    save('checkpoint-'+phase+'.json',data)
    if phase=='after':require(data==load('checkpoint-before.json'),'Data/catalog/sequence checkpoint mismatch')
    event('checkpoint_'+phase+'_verified',tables=len(counts),rows=sum(counts.values()),sequences=len(sequences))

def stopped_copy():
    s=mountcheck();require((R/'checkpoint-before.json').exists(),'Missing source checkpoint')
    pg=inspect('iot_postgres');require(pg['Id']==s['source_container_id'],'Wrong source container')
    require(all(not inspect(n)['State']['Running'] for n in WRITERS),'Writer running')
    require(not pathlib.Path(s['data_path']).exists(),'Destination already exists; review partial work')
    run('docker','stop','--signal','SIGINT','--timeout','-1','iot_postgres')
    pg=inspect('iot_postgres');require(not pg['State']['Running'] and pg['State']['ExitCode']==0 and not pg['State']['OOMKilled'],'Source did not stop cleanly')
    event('postgres_cleanly_stopped')
    mountcheck();pathlib.Path(s['data_path']).mkdir(mode=0o700)
    args=['docker','run','--rm','--network','none','--cpus','1','--memory','512m','--mount','type=volume,source='+SRC+',target=/source,readonly','--mount','type=bind,source='+s['data_path']+',target=/target','--mount','type=bind,source='+str(R)+',target=/ops,readonly',TOOLS,'bash','/ops/copy-stopped-cluster.sh','/source','/target',s['system_id'],s['filesystem_id']]
    event('physical_copy_started');started=time.monotonic();result=run(*args)
    require(result.startswith('COPY_VERIFIED:'),'Copy not verified')
    save('COPY-CLUSTER-VERIFIED.json',{'status':'PASS','seconds':time.monotonic()-started,'result':result,'original_container_id':pg['Id'],'original_volume':SRC})
    event('physical_copy_verified',seconds=round(time.monotonic()-started,1))

class DockerConnection(http.client.HTTPConnection):
    def connect(self):
        self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);self.sock.connect('/var/run/docker.sock')
def create(payload):
    c=DockerConnection('localhost',timeout=30);c.request('POST','/containers/create?name=iot_postgres',body=json.dumps(payload),headers={'Content-Type':'application/json'});response=c.getresponse();data=json.loads(response.read());c.close()
    require(response.status==201,'Docker create failed: '+str(data));return data['Id']
def ready():
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        c=inspect('iot_postgres')
        if not c['State']['Running']:
            require(c['State']['Status']=='created','Candidate stopped during startup')
            time.sleep(1);continue
        if c['State'].get('Health',{}).get('Status')=='healthy':return
        time.sleep(2)
    raise RuntimeError('Candidate health deadline exceeded')
def activate():
    s=mountcheck();require(load('COPY-CLUSTER-VERIFIED.json')['status']=='PASS','Copy not verified')
    pg=inspect('iot_postgres');require(pg['Id']==s['source_container_id'] and not pg['State']['Running'],'Source must be original and stopped')
    renderer=module('renderer','render-runtime.py');payload=renderer.render(pg,s);save('candidate-create.private.json',payload)
    require(payload['NetworkingConfig']['EndpointsConfig'][NETWORK]['Aliases']==['iot_postgres','postgres'],'Wrong network aliases')
    for src,dst,mode in [('guard-entrypoint.sh',s['guard_path'],0o755),('host-preflight.py','/usr/local/libexec/upat-pg-host-preflight.py',0o755),('production-spec.json','/etc/upat-nzc/postgres-volume.json',0o600),('upat-postgres-volume.service','/etc/systemd/system/upat-postgres-volume.service',0o644)]:
        p=pathlib.Path(dst);require(not p.exists(),'Refusing to overwrite '+dst);p.parent.mkdir(exist_ok=True);shutil.copyfile(R/src,p);p.chmod(mode)
    run('systemd-analyze','verify','/etc/systemd/system/upat-postgres-volume.service','/run/systemd/generator/mnt-HC_Volume_106884142.mount')
    run('docker','rename','iot_postgres',OLD)
    run('docker','network','disconnect',NETWORK,OLD)
    ident=create(payload);save('candidate-created.json',{'id':ident});event('candidate_created',id=ident)
    run('python3','/usr/local/libexec/upat-pg-host-preflight.py','/etc/upat-nzc/postgres-volume.json')
    run('systemctl','daemon-reload');run('systemctl','start','upat-postgres-volume.service');ready()
    require(sql('SELECT system_identifier FROM pg_control_system();')==s['system_id'],'Wrong cluster after start')
    require(run('systemctl','is-active','upat-postgres-volume.service')=='active','Service not active')
    event('candidate_healthy',id=ident)

def resume():
    require(load('checkpoint-before.json')==load('checkpoint-after.json'),'Candidate checkpoint not verified')
    require(not (R/'RESUMED.json').exists(),'Already resumed')
    ready();s=load('state-before.json');spec=mountcheck()
    require(sql('SELECT system_identifier FROM pg_control_system();')==spec['system_id'],'Wrong cluster')
    require(subprocess.check_output(['crontab','-l'])==(R/'root-crontab.paused-listing').read_bytes(),'Cron drift')
    require(not inspect(OLD)['State']['Running'],'Rollback source unexpectedly running')
    require(inspect('iot_mosquitto')['State']['Running'],'MQTT broker stopped')
    # Resolve both aliases and make a real SQL connection from the application network.
    for alias in ['postgres','iot_postgres']:
        actual=run('docker','run','--rm','--network',NETWORK,'--entrypoint','getent',spec['image'],'hosts',alias)
        require(actual.split()[0]==inspect('iot_postgres')['NetworkSettings']['Networks'][NETWORK]['IPAddress'],'Wrong DNS alias '+alias)
    require(load('NETWORK-SQL-VERIFIED.json')['status']=='PASS','Application-network SQL test missing')
    run('systemctl','enable','upat-postgres-volume.service')
    # Once this marker exists, old-copy rollback is forbidden, even if resume stops halfway.
    save('WRITERS-RELEASED.json',{'at':datetime.datetime.now(datetime.timezone.utc).isoformat()})
    for n in WRITERS:
        old=next(c for c in s['containers'] if c['name']=='/'+n);c=inspect(n)
        require(c['Id']==old['id'] and c['Image']==old['image'],'Writer identity drift')
        run('docker','start',n)
    original=(R/'root-crontab.original').read_bytes();require(hashlib.sha256(original).hexdigest()==s['cron_original_sha256'],'Original cron checksum mismatch')
    subprocess.run(['crontab','-'],input=original,check=True)
    run('systemctl','enable','--now',*TIMERS)
    save('RESUMED.json',{'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate_id':inspect('iot_postgres')['Id'],'original_retained':OLD})
    event('writers_and_schedules_resumed')

def network_check():
    require(not (R/'WRITERS-RELEASED.json').exists(), 'Network check must precede writer release')
    spec=mountcheck();results=[]
    for name in WRITERS:
        c=inspect(name);env=dict(v.split('=',1) for v in c['Config']['Env'])
        require(env['POSTGRES_HOST'] in ['postgres','iot_postgres'],'Unexpected DB hostname')
        args=['docker','run','--rm','--name','codex-volume-network-check-'+name,'--network',NETWORK,'-e','PGPASSWORD','-e','PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=10000','--entrypoint','psql',spec['image'],'-h',env['POSTGRES_HOST'],'-p',env.get('POSTGRES_INTERNAL_PORT','5432'),'-U',env['POSTGRES_USER'],'-d',env['POSTGRES_DB'],'-X','-qAt','-v','ON_ERROR_STOP=1','-c',"SELECT system_identifier::text||':'||(SELECT oid::text FROM pg_database WHERE datname=current_database()) FROM pg_control_system();"]
        reply=subprocess.check_output(args,env={**os.environ,'PGPASSWORD':env['POSTGRES_PASSWORD']},text=True).strip()
        require(reply==spec['system_id']+':16384','Wrong DB reached by '+name)
        results.append({'application':name,'authenticated_readonly_connection':'PASS'})
    save('NETWORK-SQL-VERIFIED.json',{'status':'PASS','applications':results});event('application_network_verified',applications=len(results))

def abort_before_writes():
    require(not (R/'WRITERS-RELEASED.json').exists(),'Old-copy rollback forbidden after writer release')
    state=load('state-before.json');spec=load('production-spec.json')
    cs={c['Name']:c for c in json.loads(run('docker','inspect',*run('docker','ps','-aq').split()))}
    if '/iot_postgres' not in cs:
        require(cs.get('/'+OLD,{}).get('Id')==spec['source_container_id'],'Original container missing')
        run('docker','rename',OLD,'iot_postgres')
    current=inspect('iot_postgres')
    if current['Id']!=spec['source_container_id']:
        require(inspect(OLD)['Id']==spec['source_container_id'],'Original rollback container missing')
        if pathlib.Path('/etc/systemd/system/upat-postgres-volume.service').exists():
            run('systemctl','disable','--now','upat-postgres-volume.service')
        if inspect('iot_postgres')['State']['Running']:
            run('docker','stop','--signal','SIGINT','--timeout','-1','iot_postgres')
        require(not inspect('iot_postgres')['State']['Running'],'Candidate still running')
        if NETWORK in inspect('iot_postgres')['NetworkSettings']['Networks']:
            run('docker','network','disconnect',NETWORK,'iot_postgres')
        run('docker','rename','iot_postgres','iot_postgres-volume-held-20260916')
        run('docker','rename',OLD,'iot_postgres')
    original=inspect('iot_postgres');require(original['Id']==spec['source_container_id'],'Wrong rollback source')
    if NETWORK not in original['NetworkSettings']['Networks']:
        run('docker','network','connect','--alias','postgres','--alias','iot_postgres',NETWORK,'iot_postgres')
    if not original['State']['Running']:
        control=run('docker','run','--rm','--network','none','--mount','type=volume,source='+SRC+',target=/source,readonly','--entrypoint','pg_controldata',spec['image'],'/source')
        fields={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in control.splitlines() if ':' in line}
        require(fields.get('Database cluster state')=='shut down' and fields.get('Database system identifier')==spec['system_id'],'Rollback source not clean')
        run('docker','start','iot_postgres')
    ready();require(sql('SELECT system_identifier FROM pg_control_system();')==spec['system_id'],'Wrong restored cluster')
    installed=subprocess.check_output(['crontab','-l'])
    require(installed==(R/'root-crontab.paused-listing').read_bytes(),'Cron drift prevents automatic restoration')
    for name in WRITERS:
        old=next(c for c in state['containers'] if c['name']=='/'+name);c=inspect(name)
        require(c['Id']==old['id'] and c['Image']==old['image'],'Writer identity changed')
        if not c['State']['Running']:run('docker','start',name)
    original=(R/'root-crontab.original').read_bytes();require(hashlib.sha256(original).hexdigest()==state['cron_original_sha256'],'Cron backup damaged')
    subprocess.run(['crontab','-'],input=original,check=True);run('systemctl','enable','--now',*TIMERS)
    save('ABORTED-RESTORED.json',{'status':'original services restored','at':datetime.datetime.now(datetime.timezone.utc).isoformat()});event('migration_aborted_original_restored')

if __name__=='__main__':
    require(os.geteuid()==0 and R.is_dir(),'Run only as root in prepared VPS environment')
    phase=sys.argv[1] if len(sys.argv)==2 else ''
    actions={'preflight':preflight,'pause':pause,'checkpoint-before':lambda:checkpoint('before'),'copy':stopped_copy,'activate':activate,'checkpoint-after':lambda:checkpoint('after'),'network-check':network_check,'resume':resume,'abort-before-writes':abort_before_writes}
    require(phase in actions,'Explicit valid single phase required')
    actions[phase]()
