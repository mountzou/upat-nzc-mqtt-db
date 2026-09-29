#!/usr/bin/env python3
"""Local Docker Desktop lifecycle test with inert containers, never a real DB."""
import json
import subprocess
import tempfile
import uuid
from pathlib import Path

DOCKER=['docker','--context','desktop-linux']
IMAGE='sha256:f30e3de0ac9cc938dac627ef2231099867c694b5f949fadb924c8c977428c399'
TAG='codex-compose-isolation-'+uuid.uuid4().hex[:10]
NETWORK=TAG+'-network'
DB=TAG+'-db'
OLD=TAG+'-old'
APP=TAG+'-app'


def run(args,ok=True):
    p=subprocess.run(DOCKER+args,capture_output=True,text=True,timeout=40)
    if ok and p.returncode: raise RuntimeError(p.stderr)
    return p


def state(name):
    c=json.loads(run(['inspect',name]).stdout)[0]
    return (c['Id'],c['State']['Status'],c['State']['StartedAt'],c['RestartCount'])


with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp)
    config={'name':TAG,'services':{'api':{'image':IMAGE,'container_name':APP,
              'entrypoint':['sh','-c'],'command':['sleep 600']}},
            'networks':{'default':{'external':True,'name':NETWORK}}}
    (root/'docker-compose.prod.yml').write_text(json.dumps(config))
    (root/'docker-compose.yml').symlink_to('docker-compose.prod.yml')
    compose=['compose','--project-directory',tmp,'--env-file','/dev/null','-p',TAG]
    created=[];network_created=False
    try:
        run(['network','create','--internal',NETWORK]);network_created=True
        run(['run','-d','--name',DB,'--network',NETWORK,'--network-alias','postgres','--entrypoint','sh',IMAGE,'-c','sleep 600']);created.append(DB)
        run(['create','--name',OLD,'--network','none','--label','com.docker.compose.project='+TAG,
             '--label','com.docker.compose.service=postgres','--label','com.docker.compose.oneoff=False',
             '--entrypoint','sh',IMAGE,'-c','exit 0']);created.append(OLD)
        before=(state(DB),state(OLD))
        created.append(APP)
        run(compose+['up','-d','--no-build','--pull','never','api'])
        if (state(DB),state(OLD))!=before:raise RuntimeError('unrelated DB lifecycle changed on app up')
        missing=run(compose+['up','-d','postgres'],ok=False)
        if missing.returncode==0 or 'no such service' not in missing.stderr.lower():raise RuntimeError('postgres service was not rejected')
        run(compose+['down'])
        if (state(DB),state(OLD))!=before:raise RuntimeError('unrelated DB lifecycle changed on app down')
        run(['network','inspect',NETWORK])
        print(json.dumps({'status':'PASS','scope':'local inert containers only','default_symlink_works':True,
          'app_up_down_preserves_external_db_and_old_orphan':True,'postgres_target_rejected':True,
          'external_network_retained':True,'production_contacted':False}))
    finally:
        for name in reversed(created):run(['rm','-f','-v',name],ok=False)
        if network_created:run(['network','rm',NETWORK])
