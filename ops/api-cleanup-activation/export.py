#!/usr/bin/env python3
"""Export pinned Git blobs, never the working tree, into a fresh build context."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

SOURCE = '1cd68d9246782e64779f44031b1d6fb51b01b7d3'
BASE = 'sha256:c1d1970522a08e90b571313146da7ce9b725e3341120ad424f35bdfc25c7218a'
ROOT_FILES = {'main.py', 'schemas.py', 'auth_service.py', 'database.py', 'requirements.txt'}

def git(repo, *args):
    return subprocess.check_output(['git', '--no-optional-locks', '-C', str(repo), *args])

def export(repo, destination):
    names = git(repo, 'ls-tree', '-r', '--name-only', SOURCE, '--', 'api').decode().splitlines()
    names = [n for n in names if n[4:] in ROOT_FILES or
             (n.startswith(('api/monitoring/', 'api/readers/')) and n.endswith(('.py', '.json')))]
    if len(names) != 60:
        raise RuntimeError('Pinned payload must contain exactly 60 runtime files')
    destination.mkdir(parents=True, exist_ok=False)
    manifest = {}
    for name in names:
        blob = git(repo, 'show', f'{SOURCE}:{name}')
        relative = name.removeprefix('api/')
        target = destination / 'payload' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)
        manifest[relative] = hashlib.sha256(blob).hexdigest()
    (destination / 'source-manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True)+'\n')
    shutil.copyfile(Path(__file__).with_name('Dockerfile'), destination/'Dockerfile')
    (destination/'.dockerignore').write_text('*\n!Dockerfile\n!payload/\n!payload/**\n!source-manifest.json\n')
    print(json.dumps({'source_revision': SOURCE, 'base_image': BASE, 'files': len(manifest)}))

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, required=True)
    p.add_argument('--destination', type=Path, required=True)
    args = p.parse_args()
    export(args.repo, args.destination)
