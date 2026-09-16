"""Real local Postgres proof of the exact production checkpoint/recovery path."""
import datetime as dt
import psycopg2
import pytest
from test_shelly_compact_postgres import db, connection, writer, migration, START, scalar, load, DSN
pytestmark=pytest.mark.skipif(not DSN,reason='Explicit isolated local DB required')
cutover=load('writer_cutover','ops/shelly-compact/writer-cutover.py')


def add(conn,mode='dual',value=1):
    with conn: writer.insert_measurement(conn,'d','power',value,'W',START,mode=mode)


def proof(conn):
    result=migration.verify(conn)
    result['finished_utc']=dt.datetime.now(dt.timezone.utc).isoformat()
    return result


def baseline(conn):
    add(conn)
    migration.capture(conn,'forward')
    while migration.batch(conn,'forward') is not None: pass
    return proof(conn)


def test_compact_only_tail_recovers_before_dual_resume(db):
    full=baseline(db)
    add(db,value=-0.0)
    cp=cutover.checkpoint(db,full)
    assert cp['high_water']==2
    assert scalar(db,"SELECT copied_rows FROM shelly_compact.copy_progress WHERE direction='reverse'")==0
    db.commit()
    for value in [None,-0.0,286.45]: add(db,'compact',value)
    assert scalar(db,'SELECT count(*) FROM public.shelly_measurements')==2
    db.commit()
    recovered=cutover.recover(db,cp['high_water'])
    assert recovered['rows_this_run']==3
    assert migration.verify(db)['results']['forward']['rows']==5
    assert cutover.recover(db,cp['high_water'])['rows_this_run']==0
    add(db,'dual',18)
    assert migration.verify(db)['results']['forward']['rows']==6


def test_post_snapshot_tail_conflict_prevents_checkpoint(db):
    full=baseline(db);add(db)
    with db,db.cursor() as cur: cur.execute('UPDATE shelly_compact.measurements SET value=999 WHERE id=2')
    with pytest.raises(ValueError,match='Conflicting'):cutover.checkpoint(db,full)
    assert scalar(db,"SELECT count(*) FROM shelly_compact.copy_progress WHERE direction='reverse'")==0


def test_pending_writer_prevents_checkpoint_without_partial_progress(db):
    full=baseline(db);pending=connection()
    try:
        writer.insert_measurement(pending,'d','power',2,'W',START,mode='dual')
        with pytest.raises(psycopg2.errors.LockNotAvailable):cutover.checkpoint(db,full)
        assert scalar(db,"SELECT count(*) FROM shelly_compact.copy_progress WHERE direction='reverse'")==0
    finally:pending.rollback();pending.close()


def test_stale_proof_and_duplicate_checkpoint_rejected(db):
    full=baseline(db)
    stale={**full,'finished_utc':(dt.datetime.now(dt.timezone.utc)-dt.timedelta(hours=2)).isoformat()}
    with pytest.raises(ValueError,match='recent'):cutover.checkpoint(db,stale)
    cutover.checkpoint(db,full)
    with pytest.raises(psycopg2.errors.UniqueViolation):cutover.checkpoint(db,full)


def test_reverse_conflict_is_not_silently_accepted(db):
    cp=cutover.checkpoint(db,baseline(db));add(db,'compact',19)
    with db,db.cursor() as cur:
        cur.execute("INSERT INTO public.shelly_measurements SELECT id,device_id,metric,999,unit,event_time FROM shelly_compact.readings WHERE id>%s",(cp['high_water'],))
    with pytest.raises(ValueError,match='Conflicting'):cutover.recover(db,cp['high_water'])
    assert scalar(db,"SELECT last_id FROM shelly_compact.copy_progress WHERE direction='reverse'")==cp['high_water']


def test_cli_verification_and_checkpoint_use_independent_transactions(db, monkeypatch, tmp_path):
    import sys
    baseline(db)
    identity=scalar(db,'SELECT system_identifier::text FROM pg_control_system()');db.commit()
    monkeypatch.setattr(cutover,'EXPECTED_SYSTEM_ID',identity)
    monkeypatch.setenv('SHELLY_MIGRATION_DSN',DSN)
    proof_file=tmp_path/'verify.json';checkpoint_file=tmp_path/'checkpoint.json'
    monkeypatch.setattr(sys,'argv',['writer-cutover.py','verify','--receipt',str(proof_file)])
    cutover.main()
    monkeypatch.setattr(sys,'argv',['writer-cutover.py','checkpoint','--receipt',str(checkpoint_file),'--proof',str(proof_file),'--writers-paused'])
    cutover.main()
    assert checkpoint_file.exists()
