"""Contract and accounting tests using a disposable local PostgreSQL fixture."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from threading import Event
from unittest.mock import patch

import pytest

from test_monitoring_postgres import DSN, database, client, login
from monitoring.services.energy_demand import consumption as c
from monitoring.services.energy_demand.devices import list_energy_devices_for_school

pytestmark = pytest.mark.skipif(not DSN, reason='Requires disposable local PostgreSQL')


@pytest.fixture(autouse=True)
def clear_cache(monkeypatch):
    monkeypatch.setattr(c, 'cache', c.ConsumptionCache())


def seed(database, day, school='school_10', skip=(), value=1000):
    start, end = c.bounds(day, day, None, None)
    devices = list_energy_devices_for_school(school)
    with database() as conn:
        with conn.cursor() as cur:
            for d in devices:
                for h in range(int((end-start)/c.HOUR)):
                    if (d['id'], h) in skip or ('*', h) in skip:
                        continue
                    a, b = start+h*c.HOUR, start+(h+1)*c.HOUR
                    if d['type'] == 'plug':
                        cur.execute('INSERT INTO shelly_plug_hourly_energy VALUES (%s,%s,%s,%s,1,1,NOW())', (d['id'],a,b,value))
                    else:
                        cur.execute('INSERT INTO shelly_pro3em_hourly_energy VALUES (%s,%s,%s,%s,%s,%s,%s,1,1,NOW())',
                                    (d['id'],a,b,value,value,value,3*value))
    return int((end-start)/c.HOUR)


def read(client, headers, day='2026-09-01', **params):
    return client.get('/energy/consumption/history', params=dict(
        school_id='school_10', start_date=str(day), end_date=str(day), **params), headers=headers)


@pytest.mark.parametrize('day,hours', [(date(2026,9,1),24),(date(2026,3,29),23),(date(2026,10,25),25)])
@pytest.mark.parametrize('interval', ['1h','60m','2h','7h','24h','day','168h'])
def test_school10_conservation_and_athens_dst(database, client, day, hours, interval):
    seed(database,day)
    response = read(client,login(client),day,interval=interval)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data['source_basis'] == 'submeter_sum'
    assert data['measurement_scope'] == 'assumed_school_total'
    assert data['unit'] == 'kWh' and data['timezone'] == 'Europe/Athens'
    assert data['coverage']['expected_series'] == 14  # 5 plugs + 9 independent phases
    assert data['summary']['total_energy_kwh'] == 14*hours
    assert sum(p['energy_kwh'] for p in data['points']) == 14*hours
    assert sum(p['energy_kwh'] for p in data['breakdown']) == 14*hours
    assert sum(data['points'][0]['load_energy_kwh']) == data['points'][0]['energy_kwh']
    assert data['days'][0]['coverage']['expected_hours'] == hours
    assert all(p['quality'] == 'complete' for p in data['points'])
    assert all(a['end']==b['start'] for a,b in zip(data['points'],data['points'][1:]))
    assert data['summary']['mean_hourly_energy_kwh'] == 14
    assert data['summary']['peak_hourly_energy_kwh'] == 14
    assert 'energy_wh' not in response.text and 'active_power' not in response.text
    if interval == 'day': assert len(data['points']) == 1
    if interval in ('1h','60m'): assert len(data['points']) == hours


def test_central_excludes_submeters_and_missing_does_not_change_plan(database,client):
    headers=login(client)
    with database() as conn:
        with conn.cursor() as cur: cur.execute("UPDATE app_users SET school_ids=ARRAY[]::text[], school_id=NULL, role='system_admin'")
    headers=login(client)
    seed(database,date(2026,9,1),'school_22')
    url='/energy/consumption/history?school_id=school_22&start_date=2026-09-01&end_date=2026-09-01'
    response=client.get(url,headers=headers)
    assert response.status_code==200,response.text
    data=response.json()
    assert data['source_basis']=='central_meter' and data['measurement_scope']=='whole_school'
    assert data['summary']['total_energy_kwh']==72
    with database() as conn:
        with conn.cursor() as cur: cur.execute('DELETE FROM shelly_pro3em_hourly_energy')
    c.cache=c.ConsumptionCache()
    data=client.get(url,headers=headers).json()
    assert data['source_basis']=='central_meter'
    assert data['summary']['total_energy_kwh'] is None
    assert data['coverage']['quality']=='missing'
    room=client.get(url+'&room_id=teachers',headers=headers).json()
    assert room['summary']['total_energy_kwh']==24
    assert room['measurement_scope']=='metered_loads'


@pytest.mark.parametrize('room,expected', [('teachers',3),('library',1),('computerclassroom',5),('principal1',2),('principal2',3)])
def test_room_plugs_and_cross_room_phases(database,client,room,expected):
    seed(database,date(2026,9,1))
    data=read(client,login(client),room_id=room).json()
    assert data['summary']['total_energy_kwh']==expected*24
    assert data['coverage']['expected_series']==expected
    assert all(b['room_id']==room for b in data['breakdown'])


def test_missing_null_invalid_zero_and_partial_are_distinct(database,client):
    day=date(2026,9,1)
    seed(database,day,skip=[('*',0)],value=0)
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE shelly_pro3em_hourly_energy SET a_energy_wh=NULL WHERE window_start='2026-09-01T00:00Z'")
            cur.execute("UPDATE shelly_plug_hourly_energy SET energy_wh='NaN' WHERE window_start='2026-09-01T01:00Z'")
            cur.execute("UPDATE shelly_plug_hourly_energy SET energy_wh=-1 WHERE window_start='2026-09-01T02:00Z'")
            cur.execute("UPDATE shelly_plug_hourly_energy SET energy_wh='Infinity' WHERE window_start='2026-09-01T03:00Z'")
    data=read(client,login(client)).json()
    assert data['points'][0]['energy_kwh'] is None and data['points'][0]['quality']=='missing'
    assert data['points'][1]['energy_kwh']==0 and data['points'][1]['quality']=='complete'
    assert data['points'][3]['observed_series_hours']==11 and data['points'][3]['quality']=='partial'
    assert data['points'][4]['observed_series_hours']==9
    assert data['summary']['total_energy_kwh']==0
    assert data['summary']['outside_school_hours_pct'] is None
    assert data['coverage']['quality']=='partial'


def test_school_hours_and_emissions_calculated_server_side(database,client):
    seed(database,date(2026,9,1))
    data=read(client,login(client),interval='day').json()
    assert data['summary']['during_school_hours_kwh']==7*14
    assert data['summary']['outside_school_hours_kwh']==17*14
    assert data['summary']['outside_school_hours_pct']==pytest.approx(100*17/24)
    assert data['summary']['equivalent_co2_kg']==336*data['emissions_factor_kg_per_kwh']
    assert sum(p['share_pct'] for p in data['hourly_profile'])==pytest.approx(100)


@pytest.mark.parametrize('extra', ['interval=30m','interval=90m','interval=169h','aggregate=sum','timezone=UTC',
    'metric=energy','format=compact','room_id=all_rooms','interval=1h&interval=day','start=2026-09-01T00:00Z',
    'end_date=2026-09-03','room_id=unknown'])
def test_reject_unsupported_or_ambiguous_parameters(client,extra):
    result=client.get('/energy/consumption/history?school_id=school_10&start_date=2026-09-01&end_date=2026-09-01&'+extra,
                      headers=login(client))
    assert result.status_code in (404,422),result.text


def test_authorization_before_source_or_cache_and_legacy_retirement(database,client):
    seed(database,date(2026,9,1))
    headers=login(client)
    assert read(client,{}).status_code==401
    assert read(client,headers).status_code==200
    with patch.object(c,'source_plan',side_effect=AssertionError('must authorize first')):
        r=client.get('/energy/consumption/history?school_id=school_22&start_date=2026-09-01&end_date=2026-09-01',headers=headers)
        assert r.status_code==403
    old=client.get('/energy/schools/school_10/rooms/hourly-energy?room_key=all_rooms&start=2026-08-31T21:00Z&end=2026-09-01T21:00Z',headers=headers)
    assert old.status_code==404,old.text
    retired='/energy/schools/{school_id}/rooms/hourly-energy'
    assert retired not in client.get('/openapi.json').json()['paths']
    assert client.get('/energy/schools/school_10/rooms/hourly-energy?room_key=all_rooms&format=compact',headers=headers).status_code==404


def test_instant_range_and_offset_equivalence(database,client):
    seed(database,date(2026,9,1))
    headers=login(client)
    url='/energy/consumption/history'
    params=dict(school_id='school_10',start='2026-09-01T01:00:00+03:00',end='2026-09-01T04:00:00+03:00')
    data=client.get(url,params=params,headers=headers).json()
    assert data['summary']['total_energy_kwh']==42
    with patch.object(c,'query_hourly',side_effect=AssertionError('cache should hit')):
        params.update(start='2026-08-31T22:00Z',end='2026-09-01T01:00Z')
        assert client.get(url,params=params,headers=headers).json()==data
    for start in ['2026-09-01T00:01Z','2026-09-01T00:00:00','2026-09-01']:
        params.update(start=start)
        assert client.get(url,params=params,headers=headers).status_code==422


def test_extreme_calendar_bounds_fail_as_validation_errors(client):
    headers=login(client)
    for day in ['0001-01-01', '9999-12-31']:
        assert read(client, headers, day, interval='168h').status_code == 422


def test_cache_coalesces_and_retries_after_failure():
    cache=c.ConsumptionCache(capacity=1)
    entered, release=Event(),Event()
    def build():
        entered.set(); assert release.wait(5); return object()
    with ThreadPoolExecutor(max_workers=2) as pool:
        first=pool.submit(cache.get,'a',build)
        assert entered.wait(5)
        second=pool.submit(cache.get,'a',lambda:pytest.fail('duplicate query'))
        release.set()
        assert first.result() is second.result()
    with pytest.raises(ValueError): cache.get('b',lambda:(_ for _ in ()).throw(ValueError()))
    assert cache.get('b',lambda:2)==2
    assert list(cache.entries)==['b']
