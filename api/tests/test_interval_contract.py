"""Interval wire contract and actual PostgreSQL bucketing across Athens DST."""
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import psycopg2
from psycopg2.extras import RealDictCursor
import pytest
from fastapi.testclient import TestClient

import main
from schemas import HistoryQueryParams
from monitoring.routes import indoor_environment, energy_demand
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord
from monitoring.utils.interval import parse_interval

UTC = timezone.utc
PATHS = ['/indoor_environment/devices/fixture/history', '/energy/devices/fixture/history',
         '/upat/device/fixture/history', '/shelly/device/fixture/history']

@pytest.mark.parametrize('value,expected', [('1m','1m'),('60m','1h'),('120m','2h'),('90m','90m'),('24h','24h'),('day','day')])
def test_normalization(value, expected):
    assert parse_interval(value).value == expected

@pytest.mark.parametrize('value', ['', '0m', '-1h', '1.5h', '1d', 'DAY', ' 5m', '01m', '1h;select 1', '999999999h'])
def test_invalid(value):
    with pytest.raises(ValueError): parse_interval(value)

@pytest.mark.parametrize('day,hours', [('2026-03-29T12:00:00Z',23), ('2026-10-25T12:00:00Z',25)])
def test_calendar_shift_uses_actual_day_duration(day, hours):
    instant=datetime.fromisoformat(day.replace('Z','+00:00'))
    spec=parse_interval('day'); start=spec.floor(instant)
    assert (spec.shift(start,1)-start).total_seconds()==hours*3600
    assert parse_interval('24h').floor(instant).hour==0
    assert start.hour in (21,22)

@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setitem(main.app.dependency_overrides,get_current_user,
        lambda: AuthUserRecord(username='fixture',role='system_admin'))
    monkeypatch.setattr(indoor_environment,'_enforce_environment_device_access',lambda *a:None)
    monkeypatch.setattr(energy_demand,'_enforce_energy_device_access',lambda *a:None)
    return TestClient(main.app)

@pytest.mark.parametrize('path', PATHS)
@pytest.mark.parametrize('query', [{'interval':'0m'}, {'interval':'1h','bucket_unit':'hour'},
    {'interval':'1h','bucket_size':1}, {'interval':'1h','bucket_minutes':60},
    {'bucket_unit':'hour'}, {'bucket_size':1}, {'bucket_unit':'hour','bucket_size':1},
    {'bucket_unit':''}, {'bucket_size':''}, {'bucket_minutes':60}, {'interval':''}])
def test_bad_or_mixed_contract_rejected_before_data_read(client,monkeypatch,path,query):
    connect=Mock(side_effect=AssertionError('invalid query reached SQL'))
    monkeypatch.setattr(main,'get_connection',connect)
    assert client.get(path,params=query).status_code==422
    connect.assert_not_called()

@pytest.mark.parametrize('path', PATHS)
def test_openapi_exposes_only_interval_for_bucketing(path):
    names={p['name'] for p in main.app.openapi()['paths'][path.replace('fixture','{device_id}')]['get']['parameters']}
    assert 'interval' in names
    assert not names.intersection({'bucket_unit','bucket_size','bucket_minutes'})

@pytest.mark.parametrize('module,path,function', [(energy_demand,PATHS[1],'fetch_energy_device_history')])
def test_public_canonical_requests_reach_same_service(client,monkeypatch,module,path,function):
    mock=Mock(return_value={'device_id':'fixture','count':0,'items':[]})
    monkeypatch.setattr(module,function,mock)
    for query in ({'interval':'60m'}, {'interval':'1h'}, {}):
        response=client.get(path,params=query)
        assert response.status_code==200,response.text
        assert mock.call_args.kwargs['interval']=='1h'
    assert client.get(path,params={'interval':'day'}).status_code==200
    assert mock.call_args.kwargs['interval']=='day'

def test_retired_phase_route_is_absent(client):
    assert '/energy/devices/{device_id}/pro3em-energy' not in main.app.openapi()['paths']
    assert client.get('/energy/devices/fixture/pro3em-energy').status_code == 404

@pytest.mark.parametrize('flag',['rolling_1h','rolling_24h_hourly'])
def test_explicit_interval_cannot_be_silently_ignored_by_preset(client,flag):
    assert client.get(PATHS[0],params={'interval':'day',flag:'true'}).status_code==422

@pytest.fixture()
def database(monkeypatch):
    dsn=os.getenv('MONITORING_TEST_DSN')
    if not dsn: pytest.skip('Requires disposable local PostgreSQL')
    assert 'host=127.0.0.1' in dsn and 'local-fixture-only' in dsn
    def connect():
        conn=psycopg2.connect(dsn,cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
            cur.execute("SET search_path TO interval_fixture; SET TIME ZONE 'UTC'")
        return conn
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute('''DROP SCHEMA IF EXISTS interval_fixture CASCADE; CREATE SCHEMA interval_fixture;
                SET search_path TO interval_fixture;
                CREATE TABLE upat_measurements(id BIGINT, device_id TEXT, metric TEXT, value FLOAT8, unit TEXT, event_time TIMESTAMPTZ);
                CREATE TABLE shelly_measurements(LIKE upat_measurements);
                CREATE TABLE upat_measurements_hourly(device_id TEXT,metric TEXT,unit TEXT,bucket_start TIMESTAMPTZ,value_avg FLOAT8,sample_count INTEGER);
                CREATE TABLE upat_measurements_5min(LIKE upat_measurements_hourly);
                CREATE TABLE upat_rollup_state(pipeline_name TEXT,last_measurement_id BIGINT);
                INSERT INTO upat_rollup_state VALUES('upat',1000);
            ''')
    monkeypatch.setattr(main,'get_connection',connect)
    return connect

@pytest.mark.parametrize('date,hours', [('2026-03-29',23),('2026-10-25',25)])
def test_sql_calendar_raw_and_weighted_rollup_with_unprocessed_tail(database,date,hours):
    spec=parse_interval('day'); start=spec.floor(datetime.fromisoformat(date+'T12:00:00+00:00'));end=spec.shift(start,1)
    with database() as conn:
        with conn.cursor() as cur:
            for i in range(hours):
                stamp=start+timedelta(hours=i)
                cur.execute("INSERT INTO upat_measurements_hourly VALUES('fixture','temperature','C',%s,10,2)",(stamp,))
                # IDs <= 1000 already rolled up, > 1000 form the unprocessed tail.
                for sample,value in [(0,10),(1,10),(2,40)]:
                    identifier=(i*3+sample if sample<2 else 1001+i)
                    cur.execute("INSERT INTO upat_measurements VALUES(%s,'fixture','temperature',%s,'C',%s)",
                        (identifier,value,stamp+timedelta(minutes=sample)))
            cur.execute('INSERT INTO shelly_measurements SELECT * FROM upat_measurements')
    params=HistoryQueryParams(interval='day',start=start.isoformat(),end=end.isoformat())
    for table in ('upat_measurements','shelly_measurements'):
        raw=main.fetch_device_history(table,'fixture',params)
        assert raw['count']==1 and raw['items'][0]['measurements']['temperature']['value']==20
        assert raw['items'][0]['event_time']==start
    result=main.fetch_upat_rollup_history('upat_measurements_hourly','fixture',params)
    assert result==main.fetch_device_history('upat_measurements','fixture',params)
    hourly=main.fetch_upat_rollup_history('upat_measurements_hourly','fixture',HistoryQueryParams(
        interval='1h',start=start.isoformat(),end=end.isoformat()))
    assert hourly['count']==hours and len({item['event_time'] for item in hourly['items']})==hours
    fixed=main.fetch_upat_rollup_history('upat_measurements_hourly','fixture',HistoryQueryParams(
        interval='24h',start=start.isoformat(),end=end.isoformat()))
    assert fixed['count']==2

@pytest.mark.parametrize('interval',['5m','90m','2h','7h','24h','day'])
def test_python_bucket_alignment_matches_postgres(database,interval):
    instant=datetime(2026,10,25,1,38,tzinfo=UTC);spec=parse_interval(interval)
    with database() as conn:
        with conn.cursor() as cur:
            if spec.calendar_day: cur.execute("SELECT date_trunc('day', %s::timestamptz, 'Europe/Athens') AS bucket",(instant,))
            else: cur.execute("SELECT date_bin(%s::interval, %s::timestamptz, TIMESTAMPTZ '2001-01-01 00:00:00+00') AS bucket",(spec.sql_duration,instant))
            assert cur.fetchone()['bucket']==spec.floor(instant)


def test_oversized_window_is_a_client_error(client):
    response=client.get(PATHS[0],params={'interval':'241920h','limit':1000})
    assert response.status_code==422,response.text



@pytest.mark.parametrize('parameter', ['bucket_unit', 'bucket_size', 'bucket_minutes'])
def test_retired_parameters_cannot_be_ignored_by_internal_model(parameter):
    with pytest.raises(ValueError, match='legacy bucket parameters'):
        HistoryQueryParams.model_validate({parameter: '1', 'interval': '1h'})
