"""Rules plus real SQL/API against a disposable local PostgreSQL only."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

from fastapi.testclient import TestClient
import psycopg2
from psycopg2.extras import RealDictCursor
import pytest

from main import app
from monitoring.notifications import job, repository
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord

DSN = os.getenv('NOTIFICATIONS_TEST_DSN')
AT = datetime.fromisoformat('2026-09-09T11:00:00+03:00')
ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('metric,kind,value,matches', [
    ('co2', 'hourly', 751, True), ('co2', 'hourly', 750, False),
    ('co2', 'daily', 799, True), ('co2', 'daily', 800, False),
    ('pm25', 'hourly', 11, True), ('pm25', 'hourly', 10, False),
    ('pm25', 'daily', 14, True), ('pm25', 'daily', 15, False),
])
def test_strict_rules(metric, kind, value, matches):
    start, end = job.previous_period(kind, AT)
    row = dict(metric=metric, average=value, room_label='Room A')
    assert (job.notification(row, kind, start, end) is not None) == matches


def test_athens_periods_and_current_catalog():
    start, end = job.previous_period('hourly', AT)
    assert start == datetime.fromisoformat('2026-09-09T10:00:00+03:00')
    assert end == AT
    start, end = job.previous_period('daily', datetime.fromisoformat('2026-09-10T00:10:00+03:00'))
    assert start == datetime.fromisoformat('2026-09-09T00:00:00+03:00')
    assert end == datetime.fromisoformat('2026-09-10T00:00:00+03:00')
    assignments = job.room_assignments()
    assert len({a['school_id'] for a in assignments}) == 6
    assert len({(a['school_id'], a['room_id']) for a in assignments}) == 9
    assert len([a for a in assignments if a['school_id']=='school_10' and a['room_id']=='teachers']) == 2


@pytest.fixture
def database(monkeypatch):
    if not DSN:
        pytest.skip('Requires disposable local PostgreSQL')
    assert 'host=127.0.0.1' in DSN and 'local-fixture-only' in DSN
    def connect():
        conn = psycopg2.connect(DSN, cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
            cur.execute('SET search_path TO iaq_notification_fixture')
        return conn
    with closing(psycopg2.connect(DSN)) as conn, conn:
        with conn.cursor() as cur:
            cur.execute('''DROP SCHEMA IF EXISTS iaq_notification_fixture CASCADE;
                CREATE SCHEMA iaq_notification_fixture;
                SET search_path TO iaq_notification_fixture;
                CREATE TABLE app_users(username TEXT PRIMARY KEY);
                INSERT INTO app_users VALUES ('school_10'), ('school_3'), ('other_teacher'), ('admin');
                CREATE TABLE upat_measurements_hourly (
                    device_id TEXT, metric TEXT, value_avg double precision, bucket_start timestamptz,
                    sample_count integer, PRIMARY KEY(device_id,metric,bucket_start));
            ''')
            cur.execute((ROOT/'db/migrations/016_iaq_notifications.sql').read_text())
            # Migration is safe to re-apply on the same schema.
            cur.execute((ROOT/'db/migrations/016_iaq_notifications.sql').read_text())
    monkeypatch.setattr(repository, 'get_connection', connect)
    yield connect
    with closing(psycopg2.connect(DSN)) as conn, conn:
        with conn.cursor() as cur:
            cur.execute('DROP SCHEMA iaq_notification_fixture CASCADE')


def samples(connect, rows):
    with closing(connect()) as conn, conn:
        with conn.cursor() as cur:
            cur.executemany("""INSERT INTO upat_measurements_hourly
                (device_id,metric,value_avg,bucket_start,sample_count)
                VALUES (%s,%s,%s,date_trunc('hour',%s::timestamptz),1)
                ON CONFLICT(device_id,metric,bucket_start) DO UPDATE SET
                    value_avg=(upat_measurements_hourly.value_avg * upat_measurements_hourly.sample_count
                               + EXCLUDED.value_avg) / (upat_measurements_hourly.sample_count + 1),
                    sample_count=upat_measurements_hourly.sample_count + 1""", rows)


def query(connect, sql):
    with closing(connect()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()


def run(connect, kind='hourly', at=AT, **kwargs):
    return job.run(kind, now=at, connection_factory=connect, **kwargs)


def test_equal_sensor_means_dedup_consecutive_hours_and_history(database):
    t = AT - timedelta(minutes=30)
    samples(database, [('portable-110','co2',700,t)] + [('portable-111','co2',900,t)]*3 + [
        ('portable-108','pm25',20,t), ('unassigned','co2',2000,t),
        ('portable-101','co2',950,t), ('portable-108','co2',None,t),
        ('portable-106','co2',float('nan'),t), ('portable-106','pm25',float('inf'),t)])
    first = run(database)
    assert first['notification_count'] == 3
    rows = query(database, 'SELECT * FROM iaq_notifications ORDER BY id')
    teachers = next(r for r in rows if r['room_id']=='teachers')
    assert teachers['average'] == 800 and teachers['sample_count'] == 4
    assert run(database)['status'] == 'already_checked'
    samples(database, [('portable-110','co2',1000,t + timedelta(hours=1))])
    assert run(database, at=AT+timedelta(hours=1))['notification_count'] == 1
    assert len(query(database, 'SELECT * FROM iaq_notifications')) == 4
    assert len(query(database, 'SELECT * FROM iaq_notification_runs')) == 2


def test_daily_previous_day_missing_zero_and_hourly_coexist(database):
    t = AT-timedelta(minutes=30)
    samples(database, [('portable-108','co2',900,t), ('portable-108','co2',500,t+timedelta(hours=1)),
                      ('portable-108','pm25',0,t), ('portable-101','co2',800,t),
                      ('portable-106','pm25',None,t),
                      # Next day does not change the previous day average.
                      ('portable-108','co2',3000,datetime.fromisoformat('2026-09-10T00:05:00+03:00'))])
    assert run(database)['notification_count'] == 2
    result = run(database,'daily',datetime.fromisoformat('2026-09-10T00:10:00+03:00'))
    assert result['notification_count'] == 2
    rows = query(database,"SELECT * FROM iaq_notifications WHERE period_kind='daily'")
    assert {r['metric']:r['average'] for r in rows} == {'co2':700,'pm25':0}
    assert all(r['school_id']=='school_10' for r in rows)


def test_empty_period_dry_run_and_failure_retry(database, monkeypatch):
    assert run(database)['notification_count'] == 0
    assert run(database)['status'] == 'already_checked'
    next_at = AT + timedelta(hours=1)
    samples(database,[('portable-108','co2',900,AT+timedelta(minutes=20))])
    assert run(database, at=next_at, dry_run=True)['notification_count'] == 1
    assert query(database,'SELECT * FROM iaq_notifications') == []
    assert len(query(database,'SELECT * FROM iaq_notification_runs')) == 1
    original = job.notification
    def fail(*args):
        raise RuntimeError('fixture failure')
    monkeypatch.setattr(job,'notification',fail)
    with pytest.raises(RuntimeError):
        run(database, at=next_at)
    monkeypatch.setattr(job,'notification',original)
    assert run(database, at=next_at)['notification_count'] == 1


@pytest.fixture
def client():
    previous = dict(app.dependency_overrides)
    app.dependency_overrides.pop(get_current_user, None)
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()
    app.dependency_overrides.update(previous)


def identify(username='school_10', schools=('school_10',), role='teacher'):
    app.dependency_overrides[get_current_user] = lambda: AuthUserRecord(
        username=username, role=role, school_ids=schools)


def test_api_auth_school_scope_history_and_independent_reads(database, client):
    samples(database,[(device,metric,value,AT-timedelta(minutes=10))
        for device in ('portable-108','portable-101') for metric,value in (('co2',900),('pm25',20))])
    run(database)
    assert client.get('/notifications').status_code == 401
    assert client.patch('/notifications/1/read').status_code == 401
    identify()
    response = client.get('/notifications?limit=1')
    assert response.status_code == 200 and response.headers['cache-control']=='no-store'
    first = response.json()
    assert first['unread_count']==2 and len(first['items'])==1
    assert first['items'][0]['school_id']=='school_10'
    second = client.get('/notifications',params={'before_id':first['next_before_id'],'limit':1}).json()
    assert len(second['items'])==1 and second['next_before_id'] is None
    assert second['items'][0]['id'] != first['items'][0]['id']
    item_id = first['items'][0]['id']
    receipt = client.patch(f'/notifications/{item_id}/read').json()
    assert client.patch(f'/notifications/{item_id}/read').json()==receipt
    assert client.get('/notifications').json()['unread_count']==1
    assert len(client.get('/notifications').json()['items'])==2
    other_id = query(database,"SELECT id FROM iaq_notifications WHERE school_id='school_3'")[0]['id']
    assert client.patch(f'/notifications/{other_id}/read').status_code==404
    assert client.patch('/notifications/999999/read').status_code==404
    identify('other_teacher')
    assert client.get('/notifications').json()['unread_count']==2
    identify('school_10',schools=('school_3',))
    assert all(r['school_id']=='school_3' for r in client.get('/notifications').json()['items'])
    assert client.patch(f'/notifications/{item_id}/read').status_code==404
    identify('admin',schools=(),role='system_admin')
    assert len(client.get('/notifications').json()['items'])==4
    assert client.get('/notifications?limit=101').status_code==422
    assert client.get('/notifications?before_id=0').status_code==422


def test_overlapping_job_does_not_write(database):
    with closing(database()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(74201916, 1)')
            with pytest.raises(RuntimeError, match='Another'):
                run(database)
    assert query(database,'SELECT * FROM iaq_notification_runs')==[]


def test_failure_after_notification_insert_rolls_back_everything(database):
    samples(database,[('portable-108','co2',900,AT-timedelta(minutes=10))])
    with closing(database()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE FUNCTION reject_run() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'fixture run marker failure'; END; $$;
                CREATE TRIGGER reject_run BEFORE INSERT ON iaq_notification_runs
                FOR EACH ROW EXECUTE FUNCTION reject_run();''')
    with pytest.raises(psycopg2.Error):
        run(database)
    assert query(database,'SELECT * FROM iaq_notifications')==[]
    assert query(database,'SELECT * FROM iaq_notification_runs')==[]
    with closing(database()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute('DROP TRIGGER reject_run ON iaq_notification_runs')
    assert run(database)['notification_count']==1


def test_api_empty_scope_and_database_failure(database,client,monkeypatch):
    samples(database,[('portable-108','co2',900,AT-timedelta(minutes=10))])
    run(database)
    identify(schools=())
    data=client.get('/notifications').json()
    assert data=={'items':[],'unread_count':0,'next_before_id':None}
    def unavailable():
        raise psycopg2.OperationalError('private connection details')
    monkeypatch.setattr(repository,'get_connection',unavailable)
    response=client.get('/notifications')
    assert response.status_code==503 and 'private' not in response.text
    assert response.headers['cache-control']=='no-store'


def test_delayed_hour_and_both_midnight_jobs(database):
    at = datetime.fromisoformat('2026-09-10T00:10:00+03:00')
    assert job.previous_period('hourly', at) == (
        datetime.fromisoformat('2026-09-09T23:00:00+03:00'),
        datetime.fromisoformat('2026-09-10T00:00:00+03:00'))
    # A running hourly job must not reject the independent daily check.
    with closing(database()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(74201916, 1)')
            assert run(database, 'daily', at)['status'] == 'completed'
    assert run(database, 'hourly', at)['status'] == 'completed'


def test_weighted_hourly_buckets_and_no_raw_table(database):
    with closing(database()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO upat_measurements_hourly VALUES
                ('portable-108','co2',500,'2026-09-08T10:00:00+03:00',1),
                ('portable-108','co2',900,'2026-09-08T11:00:00+03:00',9)""")
    # Weighted daily mean is 860, not mean-of-means 700 (which would notify).
    assert run(database, 'daily', AT)['notification_count'] == 0


def test_teachers_equal_sensor_daily_means_and_missing_sensor(database):
    with closing(database()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO upat_measurements_hourly VALUES
                ('portable-110','co2',500,'2026-09-08T10:00:00+03:00',1),
                ('portable-110','co2',700,'2026-09-08T11:00:00+03:00',3),
                ('portable-111','co2',900,'2026-09-08T10:00:00+03:00',100),
                ('portable-110','pm25',5,'2026-09-08T10:00:00+03:00',7)""")
    # Sensor means 650 and 900 -> room mean 775, regardless of sensor counts.
    assert run(database, 'daily', AT)['notification_count'] == 2
    rows = query(database, 'SELECT metric,average,sample_count FROM iaq_notifications ORDER BY metric')
    assert rows == [dict(metric='co2',average=775,sample_count=104),
                    dict(metric='pm25',average=5,sample_count=7)]


def test_single_sensor_uses_exact_stored_hourly_average(database):
    with closing(database()) as conn, conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO upat_measurements_hourly VALUES
                ('portable-108','co2',750.00000001,'2026-09-09T10:00:00+03:00',17)""")
    assert run(database)['notification_count'] == 1
    row = query(database, 'SELECT average FROM iaq_notifications')[0]
    assert row['average'] == 750.00000001
