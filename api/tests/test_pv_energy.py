"""Real PostgreSQL contract tests, restricted to a disposable local database."""
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import patch

import pytest
from psycopg2.extras import execute_values
from test_monitoring_postgres import DSN, database, client, insert_day, login
from monitoring.services.energy_production import energy as pv

pytestmark = pytest.mark.skipif(not DSN, reason='Requires disposable local PostgreSQL')


@pytest.fixture(autouse=True)
def energy_fixture(database, monkeypatch):
    monkeypatch.setattr(pv, 'energy_cache', pv.EnergyDayCache())
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE pv_day_ahead_forecast_runs (
                id SERIAL PRIMARY KEY, forecast_date DATE, success BOOLEAN,
                started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ);
                CREATE TABLE pv_day_ahead_forecast_hourly (
                run_id INTEGER, forecast_timestamp TIMESTAMP,
                forecast_hour INTEGER, predicted_power_kw DOUBLE PRECISION);
            ''')


def read(client, headers, start, end=None, interval='1h', kind='history', **extra):
    return client.get('/energy/production/'+kind, params={
        'start_date':str(start), 'end_date':str(end or start), 'interval':interval, **extra}, headers=headers)


def forecast(connect, day, power=6, success=True, hours=range(24), started='2026-08-01T00:00Z'):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute('INSERT INTO pv_day_ahead_forecast_runs(forecast_date,success,started_at,completed_at) VALUES (%s,%s,%s,%s) RETURNING id', (day,success,started,started))
            run = cur.fetchone()['id']
            execute_values(cur, 'INSERT INTO pv_day_ahead_forecast_hourly VALUES %s',
                [(run,datetime.combine(day,datetime.min.time())+timedelta(hours=h),h,power) for h in hours])
    return run


@pytest.mark.parametrize('day,hours', [(date(2026,9,1),24),(date(2026,3,29),23),(date(2026,10,25),25)])
@pytest.mark.parametrize('interval', ['5m','15m','1h','day','7h','24h'])
def test_energy_conservation_and_dst(database, client, day, hours, interval):
    insert_day(database,day,power=6)
    result=read(client,login(client),day,interval=interval)
    assert result.status_code==200,result.text
    payload=result.json()
    assert payload['unit']=='kWh' and payload['kind']=='history'
    assert payload['summary']['total_energy_kwh']==hours*6
    assert sum(p['energy_kwh'] for p in payload['points'])==hours*6
    assert sum(p['expected_samples'] for p in payload['points'])==hours*12
    assert all(p['quality']=='complete' for p in payload['points'])
    assert payload['days'][0]['expected_samples']==hours*12
    assert all('_kw"' not in result.text for _ in [0])
    assert 'resolution' not in payload
    if interval=='15m': assert all(p['energy_kwh']==1.5 for p in payload['points'])
    if interval=='1h': assert len(payload['points'])==hours


def test_partial_zero_missing_and_no_extrapolation(database,client):
    day=date(2026,9,1)
    insert_day(database,day,power=6,missing=(0,))
    insert_day(database,day+timedelta(days=1),power=0)
    data=read(client,login(client),day,day+timedelta(days=2)).json()
    first=data['points'][0]
    assert first['energy_kwh']==5.5 and first['observed_samples']==11 and first['quality']=='partial'
    assert data['summary']['total_energy_kwh']==143.5
    assert data['summary']['partial_day_count']==1 and data['summary']['missing_day_count']==1
    assert data['days'][1]['energy_kwh']==0 and data['days'][1]['quality']=='complete'
    assert data['days'][2]['energy_kwh'] is None and data['points'][-1]['energy_kwh'] is None


def test_fixed_buckets_cross_midnight_without_duplicate_points(database,client):
    day=date(2026,9,1)
    insert_day(database,day);insert_day(database,day+timedelta(days=1))
    data=read(client,login(client),day,day+timedelta(days=1),'7h').json()
    assert sum(p['energy_kwh'] for p in data['points'])==288
    assert len({p['start'] for p in data['points']})==len(data['points'])
    assert all(a['end']==b['start'] for a,b in zip(data['points'],data['points'][1:]))
    assert data['summary']['maximum_interval_energy_kwh']==42


def test_forecast_latest_successful_per_target_date(database,client):
    day=date(2026,9,1)
    forecast(database,day,2)
    run=forecast(database,day,6,started='2026-08-02T00:00Z')
    forecast(database,day,99,False,started='2026-08-03T00:00Z')
    headers=login(client)
    data=read(client,headers,day,interval='2h',kind='forecasts').json()
    assert data['summary']['total_energy_kwh']==144
    assert data['runs'][0]['run_id']==run
    # UTC-aligned 2h bins clip to Athens midnight at both range boundaries.
    assert all(p['energy_kwh']==6*p['expected_samples'] for p in data['points'])
    latest=client.get('/energy/production/forecasts?latest=true&interval=1h',headers=headers)
    assert latest.status_code==200 and latest.json()['start_date']==str(day)
    assert latest.json()['runs'][0]['run_id']==run
    assert 'predicted_power_kw' not in latest.text
    assert latest.json()['latest_observed_at'] is None


@pytest.mark.parametrize('day,hours,quality', [(date(2026,3,29),23,'complete'),(date(2026,10,25),24,'partial')])
def test_legacy_civil_forecast_dst_is_not_fabricated(database,client,day,hours,quality):
    forecast(database,day)
    data=read(client,login(client),day,kind='forecasts').json()
    assert data['summary']['total_energy_kwh']==hours*6
    assert data['days'][0]['quality']==quality
    assert len({p['start'] for p in data['points']})==data['days'][0]['expected_samples']
    assert sum(p['observed_samples'] for p in data['points'])==hours


@pytest.mark.parametrize('interval',['1m','7m','0m','1d','1.5h','169h'])
def test_invalid_history_intervals(client,interval):
    assert read(client,login(client),date(2026,9,1),interval=interval).status_code==422


@pytest.mark.parametrize('extra',[{'metric':'active_power'},{'aggregate':'avg'},{'resolution':'hour'},{'timezone':'UTC'}])
def test_no_unused_parameters(client,extra):
    assert read(client,login(client),date(2026,9,1),**extra).status_code==422


def test_auth_and_forecast_validation(client):
    assert read(client,{},date(2026,9,1)).status_code==401
    headers=login(client)
    assert read(client,headers,date(2026,9,1),interval='15m',kind='forecasts').status_code==422
    assert read(client,headers,date(2026,9,1),kind='forecasts',latest=True).status_code==422
    assert client.get('/energy/production/forecasts',headers=headers).status_code==422
    assert client.get('/energy/production/forecasts?latest=true',headers=headers).status_code==404
    assert client.get('/energy/production/history?start_date=2026-09-01&end_date=2026-09-01&interval=1h&interval=day',headers=headers).status_code==422


def test_cache_overlap_normalization_and_day_reuse(database,client):
    day=date(2026,9,1);insert_day(database,day)
    with patch.object(pv,'query_days',wraps=pv.query_days) as query:
        first=pv.get_energy(day,day,'60m')
        same=pv.get_energy(day,day,'1h')
        daily=pv.get_energy(day,day,'day')
        assert query.call_count==1 and first==same
        assert daily.fetched_at==first.fetched_at and len(daily.points)==1
        assert daily.summary.total_energy_kwh==first.summary.total_energy_kwh
        pv.get_energy(day,day+timedelta(days=1),'1h')
        assert query.call_count==2 and query.call_args.args[:2]==(day+timedelta(days=1),)*2


def test_inflight_coalescing_and_failure_not_cached(monkeypatch):
    cache=pv.EnergyDayCache();entered=Event();release=Event();calls=[];day=date(2026,9,1)
    def query(start,end,*args):
        calls.append(start);entered.set();assert release.wait(3)
        return {start:[]}
    monkeypatch.setattr(pv,'query_days',query)
    with ThreadPoolExecutor(2) as pool:
        a=pool.submit(cache.get,day,day,pv.parse_interval('1h'),'history');assert entered.wait(3)
        b=pool.submit(cache.get,day,day,pv.parse_interval('day'),'history');release.set()
        assert a.result()==b.result() and len(calls)==1
    def fail(*args): raise RuntimeError('fixture')
    monkeypatch.setattr(pv,'query_days',fail)
    with pytest.raises(RuntimeError):cache.get(day,day,pv.parse_interval('day'),'forecast')
    assert not cache.inflight and len(cache.entries)==1


@pytest.mark.parametrize('suffix', ['active-power','aggregates','day-ahead-forecasts'])
def test_retired_routes_are_absent_and_schema_is_energy_only(client,suffix):
    path='/energy/production/solar/'+suffix
    for headers in ({},login(client),{'Authorization':'Bearer invalid-token'}):
        retired=client.get(path,params={'start_date':'2026-09-01','end_date':'2026-09-01','resolution':'hour'},headers=headers)
        assert retired.status_code==404
    spec=client.get('/openapi.json').json()
    assert '/energy/production/history' in spec['paths']
    assert '/energy/production/forecasts' in spec['paths']
    assert not any('/energy/production/solar/' in route for route in spec['paths'])
    assert not any('Solar' in model for model in spec['components']['schemas'])
