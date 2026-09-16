#!/usr/bin/env python3
"""Certify a reverse-copy baseline, then recover only the compact-only tail."""
import argparse
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import time
import psycopg2

spec = importlib.util.spec_from_file_location('migration', Path(__file__).with_name('migrate.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def checkpoint(conn, proof):
    if proof.get('status') != 'PASS' or proof['results']['forward'] != proof['results']['reverse']:
        raise ValueError('A full equal binary snapshot is required')
    age = (dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(proof['finished_utc'])).total_seconds()
    if not 0 <= age < 3600:
        raise ValueError('Full verification is no longer recent')
    verified = proof['results']['forward']['max_id']
    with conn, conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout='1s'; SET LOCAL statement_timeout='30s'; SET LOCAL max_parallel_workers_per_gather=0")
        # Caller has stopped the only measurement writer. Drain in-flight inserts.
        cur.execute('LOCK TABLE public.shelly_measurements, shelly_compact.measurements IN SHARE MODE')
        cur.execute("SELECT high_water,last_id FROM shelly_compact.copy_progress WHERE direction='forward'")
        forward = cur.fetchone()
        if not forward or forward[0] != forward[1] or verified < forward[0]:
            raise ValueError('Forward copy is incomplete or verification predates it')
        cur.execute('SELECT (SELECT max(id) FROM public.shelly_measurements),(SELECT max(id) FROM shelly_compact.measurements)')
        legacy, compact = cur.fetchone()
        if legacy != compact or compact < verified:
            raise ValueError('Measurement heads diverged')
        # Start at the original drained-writer barrier, not the recent snapshot's
        # max ID: allocation order is not commit order during dual ingestion.
        m.verify_range(cur, forward[0], compact)
        cur.execute("INSERT INTO shelly_compact.copy_progress(direction,high_water,last_id,copied_rows) VALUES ('reverse',%s,%s,0)", (compact,compact))
    return {'status':'PASS','high_water':compact,'tail_verified_from':forward[0],
            'full_snapshot':proof['results']['forward'],'method':'certified equal baseline; no historical reverse copy'}


def recover(conn, baseline, space_path=None):
    with conn, conn.cursor() as cur:
        cur.execute("SELECT last_id FROM shelly_compact.copy_progress WHERE direction='reverse'")
        row=cur.fetchone()
        if not row or row[0] < baseline:
            raise ValueError('Missing certified reverse baseline')
    high=m.capture(conn,'reverse')
    rows=0
    while True:
        if space_path: m.check_headroom(conn,space_path)
        result=m.batch(conn,'reverse',5000)
        if result is None: break
        rows+=result['rows']
        time.sleep(.05)
    with conn, conn.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout='1s'; SET LOCAL statement_timeout='60s'")
        cur.execute('LOCK TABLE public.shelly_measurements, shelly_compact.measurements IN SHARE MODE')
        m.verify_range(cur,baseline,high)
        cur.execute('SELECT (SELECT max(id) FROM public.shelly_measurements),(SELECT max(id) FROM shelly_compact.measurements)')
        if cur.fetchone() != (high,high):
            raise ValueError('Recovered heads differ; keep writers stopped')
    return {'status':'PASS','baseline':baseline,'high_water':high,'rows_this_run':rows,'tail_binary_equality':True}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase',choices=['verify','checkpoint','recover'])
    p.add_argument('--receipt',type=Path,required=True)
    p.add_argument('--proof',type=Path)
    p.add_argument('--baseline',type=int)
    p.add_argument('--writers-paused',action='store_true')
    a=p.parse_args()
    if a.phase != 'verify' and not a.writers_paused: p.error('Drain and stop writer first')
    if a.receipt.exists(): p.error('Receipt already exists')
    with psycopg2.connect(os.environ['SHELLY_MIGRATION_DSN'],application_name='shelly_writer_cutover') as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT system_identifier::text FROM pg_control_system()')
            if cur.fetchone() != ('7618955918777626661',): raise ValueError('Wrong production cluster')
        conn.commit()
        if a.phase=='verify': result=m.verify(conn)
        elif a.phase=='checkpoint': result=checkpoint(conn,json.loads(a.proof.read_text()))
        else:
            if a.baseline is None: p.error('Certified baseline required')
            result=recover(conn,a.baseline,'/space-check')
    result['finished_utc']=dt.datetime.now(dt.timezone.utc).isoformat()
    with a.receipt.open('x') as f:
        json.dump(result,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    print(json.dumps(result),flush=True)


if __name__=='__main__': main()
