"""Synthetic SQL integration; refuses any database except the named local fixture."""
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pytest
from fastapi import HTTPException
import main
from monitoring.services.environment.history import resolve_history_plan, get_environmental_history

DSN=os.getenv('ENVIRONMENT_HISTORY_TEST_DSN')
pytestmark=pytest.mark.skipif(not DSN,reason='Requires isolated local PostgreSQL fixture')
NOW=datetime(2027,1,1,tzinfo=timezone.utc)

def dt(s):return datetime.fromisoformat(s.replace('Z','+00:00'))

@pytest.fixture
def db(monkeypatch):
    assert DSN and 'host=127.0.0.1' in DSN and 'dbname=environment_history_fixture' in DSN and 'password=local-fixture-only' in DSN
    def connect():return psycopg2.connect(DSN,cursor_factory=RealDictCursor)
    with connect() as c,c.cursor() as q:
        q.execute('DROP TABLE IF EXISTS upat_measurements,upat_measurements_5min,upat_measurements_hourly,upat_rollup_state')
        q.execute('''CREATE TABLE upat_measurements(id bigint PRIMARY KEY,device_id text,metric text,value double precision,unit text,event_time timestamptz);
        CREATE TABLE upat_rollup_state(pipeline_name text PRIMARY KEY,last_measurement_id bigint);
        INSERT INTO upat_rollup_state VALUES ('upat',0);
        CREATE TABLE upat_measurements_5min(device_id text,metric text,bucket_start timestamptz,value_avg double precision,sample_count integer,unit text,PRIMARY KEY(device_id,metric,bucket_start));
        CREATE TABLE upat_measurements_hourly(LIKE upat_measurements_5min INCLUDING ALL);
        CREATE INDEX ON upat_measurements(device_id,metric,event_time);
        ''')
    monkeypatch.setattr(main,'get_connection',connect)
    return connect

def insert(db, rows):
    with db() as c,c.cursor() as q:
        execute_values(q,'INSERT INTO upat_measurements VALUES %s',[(i,'fixture',metric,v,'C' if metric=='temperature' else 'ppm',dt(t)) for i,t,v,metric in rows])

def rollup(db, watermark):
    with db() as c,c.cursor() as q:
        for table,stride in [('upat_measurements_5min','5 minutes'),('upat_measurements_hourly','1 hour')]:
            q.execute(f'''INSERT INTO {table} SELECT device_id,metric,date_bin(%s::interval,event_time,TIMESTAMPTZ '2001-01-01 00:00Z'),AVG(value),COUNT(*),MAX(unit)
            FROM upat_measurements WHERE id <= %s AND value IS NOT NULL GROUP BY device_id,metric,3''',(stride,watermark))
        q.execute("UPDATE upat_rollup_state SET last_measurement_id=%s",(watermark,))

def history(start,end,**kw):
    return get_environmental_history('fixture',resolve_history_plan(start=dt(start),end=dt(end),now=NOW,**kw))

def test_exact_rolling_window_weighted_raw_edges_and_late_tail(db):
    rows=[(1,'2026-09-07T12:16:00+03:00',1000,'temperature'),
          (2,'2026-09-07T12:17:00+03:00',10,'temperature'),
          (3,'2026-09-07T12:19:00+03:00',20,'temperature'),
          (4,'2026-09-07T12:21:00+03:00',30,'temperature'),
          (5,'2026-09-07T12:22:00+03:00',30,'temperature'),
          (6,'2026-09-07T13:16:59+03:00',50,'temperature'),
          (7,'2026-09-07T13:17:00+03:00',900,'temperature')]
    insert(db,rows);rollup(db,7)
    insert(db,[(8,'2026-09-07T12:22:30+03:00',100,'temperature')])
    result=history('2026-09-07T12:17+03:00','2026-09-07T13:17+03:00',alignment='window')
    reading=result.items[0].measurements['temperature']
    assert result.count==1 and reading.sample_count==6
    assert reading.value==pytest.approx(240/6)
    assert result.items[0].event_time.isoformat()=='2026-09-07T12:17:00+03:00'
    assert result.items[0].end.isoformat()=='2026-09-07T13:17:00+03:00'

def test_hourly_unequal_counts_zero_null_and_metrics(db):
    insert(db,[(1,'2026-09-01T00:01Z',0,'temperature'),(2,'2026-09-01T00:06Z',90,'temperature'),(3,'2026-09-01T00:07Z',90,'temperature'),(4,'2026-09-01T00:08Z',None,'temperature'),(5,'2026-09-01T00:09Z',700,'co2')]);rollup(db,5)
    result=history('2026-09-01T00:00Z','2026-09-01T01:00Z',metric=['temperature'])
    assert set(result.items[0].measurements)=={'temperature'}
    assert result.items[0].measurements['temperature'].value==60
    assert result.items[0].measurements['temperature'].sample_count==3

def test_missing_buckets_not_zero_and_limit_counts_requested_slots(db):
    insert(db,[(1,'2026-09-01T00:01Z',0,'temperature')]);rollup(db,1)
    result=history('2026-09-01T00:00Z','2026-09-01T03:00Z',limit=3)
    assert result.count==1 and result.items[0].measurements['temperature'].value==0
    with pytest.raises(HTTPException) as err:history('2026-09-01T00:00Z','2026-09-01T03:00Z',limit=2)
    assert err.value.status_code==422
    assert history('2026-09-02T00:00Z','2026-09-02T01:00Z').items==[]

def test_retained_rollups_work_and_unrecoverable_edges_fail(db):
    insert(db,[(1,'2026-09-01T00:01Z',10,'temperature'),(2,'2026-09-01T00:02Z',20,'temperature')]);rollup(db,2)
    with db() as c,c.cursor() as q:q.execute('DELETE FROM upat_measurements')
    assert history('2026-09-01T00:00Z','2026-09-01T01:00Z').items[0].measurements['temperature'].value==15
    assert history('2026-09-01T00:00Z','2026-09-01T00:05Z',alignment='window',interval='5m').items[0].measurements['temperature'].value==15
    with pytest.raises(HTTPException) as err:history('2026-09-01T00:02Z','2026-09-01T00:07Z',alignment='window',interval='5m')
    assert err.value.status_code==422 and 'no longer retained' in err.value.detail

def test_adjacent_unaligned_buckets_equal_raw_oracle(db):
    start=dt('2026-09-01T00:17Z');rows=[(i+1,(start+timedelta(minutes=i)).isoformat(),float(i),'temperature') for i in range(22)]
    insert(db,rows);rollup(db,18)
    result=history(start.isoformat(),(start+timedelta(minutes=22)).isoformat(),alignment='window',interval='7m')
    assert [r.measurements['temperature'].sample_count for r in result.items]==[1,7,7,7]
    assert [r.measurements['temperature'].value for r in result.items]==[21,17,10,3]

@pytest.mark.parametrize('day,hours',[('2026-03-29',23),('2026-10-25',25)])
def test_athens_clock_dst_has_distinct_hour_buckets(db,day,hours):
    zone=ZoneInfo('Europe/Athens');start=datetime.fromisoformat(day).replace(tzinfo=zone);end=start+timedelta(days=1)
    lo,hi=start.astimezone(timezone.utc),end.astimezone(timezone.utc)
    insert(db,[(i+1,(lo+timedelta(hours=i,minutes=1)).isoformat(),i,'temperature') for i in range(hours)]);rollup(db,hours)
    hourly=history(start.isoformat(),end.isoformat(),limit=hours)
    assert hourly.count==hours
    assert len({x.event_time.isoformat() for x in hourly.items})==hours
    daily=history(start.isoformat(),end.isoformat(),interval='day',limit=1)
    assert daily.items[0].measurements['temperature'].value==pytest.approx((hours-1)/2)
    assert daily.items[0].measurements['temperature'].sample_count==hours

def test_missing_watermark_fails_closed(db):
    with db() as c,c.cursor() as q:q.execute('DELETE FROM upat_rollup_state')
    with pytest.raises(HTTPException) as err:history('2026-09-01T00:00Z','2026-09-01T01:00Z')
    assert err.value.status_code==503
