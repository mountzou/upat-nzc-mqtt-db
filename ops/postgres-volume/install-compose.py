#!/usr/bin/env python3
"""Install committed config files only; never execute Compose lifecycle commands."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path('/opt/upat-nzc-mqtt-db')
FILES=('docker-compose.prod.yml','ops/postgres-volume/check-compose.py','ops/postgres-volume/COMPOSE-OWNERSHIP.md')


def require(ok,reason):
    if not ok:raise RuntimeError(reason)


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic(path,data,mode=0o644):
    tmp=path.with_name('.'+path.name+'.volume-compose-new')
    require(not tmp.exists() and not tmp.is_symlink(),'temporary file already exists')
    with tmp.open('xb') as f:
        f.write(data);f.flush();os.fsync(f.fileno())
    os.chmod(tmp,mode);os.replace(tmp,path)
    fd=os.open(str(path.parent),os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def load_checker(stage):
    spec=importlib.util.spec_from_file_location('checker',stage/'ops/postgres-volume/check-compose.py')
    checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)
    return checker


def main(stage):
    os.umask(0o077)
    require(os.geteuid()==0,'root required')
    manifest=json.loads((stage/'ARTIFACT-MANIFEST.json').read_text())
    for rel,digest in manifest['files'].items():
        require(not Path(rel).is_absolute() and '..' not in Path(rel).parts,'invalid artifact path')
        require(sha(stage/rel)==digest,'artifact checksum mismatch: '+rel)
    require(all(rel in manifest['files'] for rel in FILES),'incomplete artifact manifest')
    for rel,digest in manifest['expected_before'].items():
        require(not (ROOT/rel).is_symlink(),'source unexpectedly symlinked: '+rel)
        require(sha(ROOT/rel)==digest,'source changed since audit: '+rel)
    checker=load_checker(stage)
    pre=checker.verify(ROOT,stage/'docker-compose.prod.yml',ROOT/'docker-compose.prod.yml')
    before=checker.stable_state(checker.inspect_all())
    env_sha=sha(ROOT/'.env')
    save=stage/'before';save.mkdir(mode=0o700)
    targets=(*FILES,'docker-compose.yml')
    previous={}
    for rel in targets:
        path=ROOT/rel
        require(not path.is_symlink(),'unexpected symlink before install: '+rel)
        if path.exists():
            previous[rel]={'mode':path.stat().st_mode&0o777,'sha256':sha(path)}
            saved=save/rel;saved.parent.mkdir(parents=True,exist_ok=True)
            saved.write_bytes(path.read_bytes())
        else:previous[rel]=None
    (save/'MANIFEST.json').write_text(json.dumps(previous,indent=2)+'\n')
    # Detect an operator edit during preflight before replacing either entrypoint.
    for rel,digest in manifest['expected_before'].items():
        require(sha(ROOT/rel)==digest,'source changed during preflight: '+rel)
    installed=[]
    try:
        for rel in FILES:
            dst=ROOT/rel;dst.parent.mkdir(parents=True,exist_ok=True)
            atomic(dst,(stage/rel).read_bytes());installed.append(rel)
        # One canonical production model, including for bare `docker compose`.
        link=ROOT/'.docker-compose.yml.volume-compose-link'
        require(not link.exists() and not link.is_symlink(),'temporary symlink already exists')
        link.symlink_to('docker-compose.prod.yml')
        os.replace(link,ROOT/'docker-compose.yml');installed.append('docker-compose.yml')
        post=checker.verify(ROOT,ROOT/'docker-compose.prod.yml',save/'docker-compose.prod.yml',True)
        require(sha(ROOT/'.env')==env_sha,'environment file changed')
        require(checker.stable_state(checker.inspect_all())==before,'container state changed across config installation')
        require(all(sha(ROOT/rel)==manifest['files'][rel] for rel in FILES),'installed checksums mismatch')
    except BaseException:
        for rel in reversed(installed):
            if previous[rel] is not None:atomic(ROOT/rel,(save/rel).read_bytes(),previous[rel]['mode'])
            else:(ROOT/rel).unlink()
        (stage/'CONFIG-ROLLBACK.json').write_text(json.dumps({'config_files_restored':True,'services_restarted':False})+'\n')
        raise
    receipt={'status':'PASS','commit':manifest['commit'],'installed_sha256':{r:sha(ROOT/r) for r in FILES},
             'default_symlink':os.readlink(ROOT/'docker-compose.yml'),'before_backup':str(save),
             'precheck':pre,'postcheck':post,'containers_unchanged':True,'env_unchanged':True,
             'lifecycle_commands_executed':False,'old_database_deleted':False}
    (stage/'INSTALLED-VERIFIED.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt,indent=2))


if __name__=='__main__':
    try:main(Path(sys.argv[1]).resolve(strict=True))
    except Exception as e:
        # Keep command output / credentials out of terminal diagnostics.
        print(json.dumps({'status':'FAIL','error_type':type(e).__name__,'message':str(e) if isinstance(e,RuntimeError) else 'Private details suppressed'}))
        sys.exit(1)
