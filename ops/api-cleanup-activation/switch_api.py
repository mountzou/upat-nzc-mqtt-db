#!/usr/bin/env python3
"""Guarded, image-only switch of the canonical production API with exact rollback.

Preparation creates a private backup. Activation/rollback always require explicit
subcommands. No database, Caddy, timer, build, pull, prune or volume operations.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
import urllib.request

ROOT = Path('/opt/upat-nzc-mqtt-db')
COMPOSE = ROOT/'docker-compose.prod.yml'
BASE = 'sha256:c1d1970522a08e90b571313146da7ce9b725e3341120ad424f35bdfc25c7218a'
OLD = 'schoolheroz-api:compact-only-overlay-0ac942b9b89e'
BASE_COMPOSE_SHA = '86bb4e9c0370429ab7e9b0f82a3e184ccaa1bc26b343d76351d2a99d7fe9ed69'
OTHERS = ['iot_postgres', 'iot_caddy', 'shelly_ingestor', 'ttn_ingestor', 'iot_mosquitto', 'nzc_energyplus']

def run(args, **kwargs):
    # Capture output: resolved Compose and Docker inspect can contain credentials.
    p = subprocess.run(args, capture_output=True, **kwargs)
    if p.returncode:
        raise RuntimeError('Command failed (private output suppressed): '+str(args[0]))
    return p.stdout

def digest(data):
    return hashlib.sha256(data).hexdigest()

def stream_digest(file):
    value = hashlib.sha256()
    for chunk in iter(lambda:file.read(1024*1024),b''): value.update(chunk)
    return value.hexdigest()

def dump(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':')).encode()

def compose_args(path=COMPOSE):
    return ['docker','compose','--project-directory',str(ROOT),'--env-file',str(ROOT/'.env'),
            '-p','upat-nzc-mqtt-db','-f',str(path)]

def model(path):
    return json.loads(run(compose_args(path)+['--profile','*','config','--format','json']))

def image_only(before, after, old_ref, new_ref):
    # JSON round-trip isolates caller-owned data and checks the entire resolved model.
    expected = json.loads(json.dumps(before))
    if expected['services']['api']['image'] != old_ref or old_ref == new_ref:
        raise RuntimeError('Unexpected baseline API image')
    expected['services']['api']['image'] = new_ref
    if expected != after:
        raise RuntimeError('Resolved Compose changed outside services.api.image')

def inspect(name):
    return json.loads(run(['docker','inspect',name]))[0]

def image_id(name):
    return json.loads(run(['docker','image','inspect',name]))[0]['Id']

def stable(container):
    value = {k:container[k] for k in ['Id','Image','RestartCount','Config','HostConfig']}
    value['StartedAt'] = container['State']['StartedAt']
    value['Mounts'] = sorted(container['Mounts'], key=lambda m:m['Destination'])
    value['Networks'] = container['NetworkSettings']['Networks']
    return digest(dump(value))

def protected():
    return {'containers':{n:stable(inspect(n)) for n in OTHERS},
            'env':digest((ROOT/'.env').read_bytes()),
            'caddy_file':digest((ROOT/'caddy/Caddyfile').read_bytes()),
            'caddy_loaded':digest(run(['docker','exec','iot_caddy','wget','-q','-O','-',
                                      'http://127.0.0.1:2019/config/'])),
            'compose_link':os.readlink(ROOT/'docker-compose.yml')}

def validator():
    run(['python3','-B',str(ROOT/'ops/postgres-volume/check-compose.py'),
         '--root',str(ROOT),'--candidate',str(COMPOSE),'--check-default'])

def no_active_jobs():
    for kind in ['hourly','daily']:
        state = run(['systemctl','show',f'upat-iaq-notifications@{kind}.service',
                     '--property=ActiveState','--value']).decode().strip()
        if state not in ('inactive','failed'):
            raise RuntimeError('IAQ job is active')

def atomic_write(path, data):
    s = path.stat()
    fd, name = tempfile.mkstemp(prefix='.api-image-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
            os.fchmod(f.fileno(), stat.S_IMODE(s.st_mode)); os.fchown(f.fileno(),s.st_uid,s.st_gid)
        os.replace(name,path)
        directory = os.open(path.parent,os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(name): os.unlink(name)

def load_record(backup):
    if backup.is_symlink() or backup.stat().st_uid != 0 or stat.S_IMODE(backup.stat().st_mode) != 0o700:
        raise RuntimeError('Backup must be a root-owned private directory')
    return json.loads((backup/'record.json').read_text())

def verify_archive(archive, expected_id):
    """Verify the complete OCI descriptor graph, including every saved layer.

    With Docker's containerd store, inspect.Id is the OCI index digest rather
    than the config blob digest. Never compare those two different identities.
    """
    with tarfile.open(archive) as tf:
        index = json.load(tf.extractfile('index.json'))
        roots = [d for d in index['manifests'] if d['digest'] == expected_id]
        if len(roots) != 1:
            raise RuntimeError('Archive does not reference the pinned OCI image')
        checked = set()
        def verify(desc):
            identity = desc['digest']
            if identity in checked: return
            if not re.fullmatch(r'sha256:[0-9a-f]{64}',identity):
                raise RuntimeError('Unsupported OCI descriptor digest')
            member = tf.getmember('blobs/sha256/'+identity.split(':')[1])
            if not member.isfile() or member.size != desc['size']:
                raise RuntimeError('OCI blob type/size mismatch')
            with tf.extractfile(member) as f:
                actual = stream_digest(f)
            if 'sha256:'+actual != identity:
                raise RuntimeError('OCI blob checksum mismatch')
            checked.add(identity)
            if desc['mediaType'].endswith(('index.v1+json','manifest.v1+json')):
                obj = json.load(tf.extractfile(member))
                for child in obj.get('manifests',[]) + obj.get('layers',[]): verify(child)
                if 'config' in obj: verify(obj['config'])
        verify(roots[0])
        return len(checked)

def prepare(backup, candidate):
    os.umask(0o077)
    if COMPOSE.is_symlink() or (ROOT/'docker-compose.yml').readlink() != Path('docker-compose.prod.yml'):
        raise RuntimeError('Canonical Compose layout changed')
    old = COMPOSE.read_bytes()
    if digest(old) != BASE_COMPOSE_SHA or image_id(OLD) != BASE or inspect('iot_api')['Image'] != BASE:
        raise RuntimeError('Pinned production baseline drifted')
    if not re.fullmatch(r'schoolheroz-api:cleanup-1cd68d9-[0-9a-f]{7,40}',candidate):
        raise RuntimeError('Unexpected candidate tag')
    candidate_id = image_id(candidate)
    if candidate_id == BASE: raise RuntimeError('Candidate equals baseline')
    validator(); no_active_jobs()
    baseline = model(COMPOSE)
    # Match exactly one image line; resolved-model comparison below is authoritative.
    pattern = rb'(?m)^(\s*image:\s*)'+re.escape(OLD.encode())+rb'(\s*(?:#.*)?)$'
    proposed, count = re.subn(pattern,lambda m:m[1]+candidate.encode()+m[2],old)
    if count != 1: raise RuntimeError('Expected one unambiguous API image line')
    before = protected()
    backup.mkdir(parents=False, exist_ok=False, mode=0o700)
    for src,name in [(COMPOSE,'compose.before.yml'),(ROOT/'.env','env.before'),
                     (ROOT/'caddy/Caddyfile','Caddyfile.before')]:
        shutil.copy2(src,backup/name); os.chmod(backup/name,0o600)
    (backup/'compose.candidate.yml').write_bytes(proposed)
    image_only(baseline,model(backup/'compose.candidate.yml'),OLD,candidate)
    archive = backup/'baseline-image.tar'
    run(['docker','image','save','--output',str(archive),OLD],timeout=120)
    archive_blobs = verify_archive(archive,BASE)
    with archive.open('rb') as f: archive_hash = stream_digest(f)
    if protected() != before or COMPOSE.read_bytes() != old: raise RuntimeError('Production drift during backup')
    record = dict(candidate=candidate,candidate_id=candidate_id,base_image=BASE,base_ref=OLD,
                  before_sha256=digest(old),candidate_sha256=digest(proposed),
                  archive_sha256=archive_hash,archive_verified_blobs=archive_blobs,protected=before,api_before=inspect('iot_api'))
    (backup/'record.json').write_bytes(dump(record))
    print(json.dumps({k:v for k,v in record.items() if k not in ('api_before','protected')}))

def switch(backup, rollback=False):
    record=load_record(backup)
    old=(backup/'compose.before.yml').read_bytes()
    new=(backup/'compose.candidate.yml').read_bytes()
    if digest(old)!=record['before_sha256'] or digest(new)!=record['candidate_sha256']:
        raise RuntimeError('Backup contents changed')
    if protected()!=record['protected']: raise RuntimeError('Protected production state changed')
    expected=new if rollback else old
    target=old if rollback else new
    if COMPOSE.read_bytes()!=expected: raise RuntimeError('Canonical Compose drifted; refusing overwrite')
    if image_id(OLD)!=BASE or image_id(record['candidate'])!=record['candidate_id']:
        raise RuntimeError('Image reference drifted')
    no_active_jobs()
    image_only(model(backup/'compose.before.yml'),model(backup/'compose.candidate.yml'),OLD,record['candidate'])
    if not rollback: validator()
    atomic_write(COMPOSE,target)
    # If this command or a postcondition fails, the backed-up old configuration is
    # retained; invoke rollback explicitly after checking protected state.
    run(compose_args()+['up','-d','--no-deps','--no-build','--pull','never','api'],timeout=120)
    expected_id=BASE if rollback else record['candidate_id']
    deadline=time.monotonic()+120
    schema=None
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen('http://127.0.0.1:8000/openapi.json',timeout=5) as response:
                schema=json.load(response)
            break
        except Exception: time.sleep(2)
    expected_count=53 if rollback else 45
    if schema is None or len(schema['paths'])!=expected_count or inspect('iot_api')['Image']!=expected_id:
        raise RuntimeError('API startup acceptance failed')
    validator()
    if protected()!=record['protected']: raise RuntimeError('Protected production state changed')
    print(json.dumps({'action':'rollback' if rollback else 'activate','image_id':expected_id,
                      'container_id':inspect('iot_api')['Id'],'paths':expected_count,'protected_unchanged':True}))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['prepare','activate','rollback'])
    p.add_argument('--backup',type=Path,required=True)
    p.add_argument('--candidate')
    a=p.parse_args()
    if os.geteuid()!=0: p.error('Must run as root on the intended VPS')
    if a.action=='prepare':
        if not a.candidate: p.error('prepare requires --candidate')
        prepare(a.backup,a.candidate)
    else: switch(a.backup,rollback=a.action=='rollback')
