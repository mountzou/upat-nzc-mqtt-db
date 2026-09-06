import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from main import app
from monitoring.routes.auth import get_current_user
from monitoring.schemas import CompactConsumptionResponse
from monitoring.services.authentication import AuthUserRecord
from monitoring.services import service_energy_api as service
from monitoring.services.consumption_compact import build_compact_consumption

START = datetime(2026, 9, 1)
PLAN = [('plug-1', None, 'Plug'), ('meter', 'a', 'Lighting'), ('meter', 'b', 'AC')]
FACTOR = dict(emissions_factor_kg_per_kwh=.4, emissions_factor_source='fixture',
              emissions_factor_reference_year=2025, emissions_factor_version='fixture-v1')


def rows(hours=24):
    result = []
    for hour in range(hours):
        for did, values in [('plug-1', {'total': hour * 10.5}), ('meter', {'total': 9000, 'a': 100, 'b': 200, 'c': 8700})]:
            result.append(dict(device_id=did, window_start=(START+timedelta(hours=hour)).isoformat()+'Z',
                               window_end=(START+timedelta(hours=hour+1)).isoformat()+'Z', energy_wh=values))
    return result


def compact(items, start=START, end=None, plan=PLAN, working_only=False):
    return build_compact_consumption(items, room_key='teachers', device_ids=['plug-1', 'meter'],
        series_plan=plan, start=start, end=end or start+timedelta(hours=24), working_only=working_only, emissions=FACTOR)


@pytest.mark.parametrize('hours', [24, 168, 169, 720])
def test_parity_and_payload_reduction(hours):
    items = rows(hours)
    old = service.aggregate_shelly_hourly_items_by_window_and_device(items, series_plan=PLAN)
    new = compact(items, end=START+timedelta(hours=hours))
    validated = CompactConsumptionResponse.model_validate(new).model_dump()
    assert [p['energy_wh_total'] for p in old] == [p['energy_wh_total'] for p in new['points']]
    assert new['summary']['total_energy_kwh'] == sum(p['energy_wh_total']/1000 for p in old)
    assert new['summary']['mean_hourly_energy_kwh'] == new['summary']['total_energy_kwh']/hours
    assert new['summary']['peak_hourly_energy_kwh'] == max(p['energy_wh_total']/1000 for p in old)
    assert new['summary']['equivalent_co2_kg'] == new['summary']['total_energy_kwh']*.4
    for load in new['breakdown']:
        assert load['energy_kwh'] == pytest.approx(sum(d['energy_wh']/1000 for p in old for d in p['by_device'] if d['device_id']==load['device_id']), rel=1e-12, abs=1e-12)
    assert new['coverage']['complete_hour_count'] == hours
    assert all('by_device' not in p for p in validated['points'])
    assert len(json.dumps(new['points'])) < len(json.dumps(old)) * .55


def test_missing_series_zero_and_empty_are_distinct():
    new = compact([rows(1)[0]])  # plug measured 0, both phases absent
    assert new['summary']['total_energy_kwh'] == 0
    assert new['summary']['measured_hour_count'] == 1
    assert new['coverage'] == dict(expected_hour_count=24, observed_hour_count=1, complete_hour_count=0, expected_series_count=3)
    assert sorted(x['observed_hour_count'] for x in new['breakdown']) == [0, 0, 1]
    assert sum(x['energy_kwh'] is None for x in new['breakdown']) == 2
    empty = compact([])
    assert empty['summary']['total_energy_kwh'] is None
    assert empty['summary']['peak_hourly_energy_kwh'] is None
    assert empty['points'] == []
    assert compact(rows(1), plan=[])['summary']['total_energy_kwh'] is None


@pytest.mark.parametrize('start,end,hours', [(datetime(2026,3,29),datetime(2026,3,30),23), (datetime(2026,10,25),datetime(2026,10,26),25), (datetime(2026,10,22),datetime(2026,10,29),169)])
def test_dst_expected_hours(start,end,hours):
    utc=lambda x: service.normalize_api_window_dt(x.replace(tzinfo=ZoneInfo('Europe/Athens')))
    assert compact([], start=utc(start), end=utc(end))['coverage']['expected_hour_count'] == hours


def test_fully_contained_hours_and_working_policy():
    new = compact(rows(24), start=START+timedelta(minutes=30), end=START+timedelta(hours=3, minutes=30))
    assert new['coverage']['expected_hour_count'] == 2
    assert new['count'] == 2
    working = compact(rows(24), working_only=True)
    expected = [p for p in rows(24)[::2] if service._is_school_hourly_energy_item(p)]
    assert working['count'] == len(expected)
    assert working['coverage']['expected_hour_count'] == len(expected)


@pytest.mark.parametrize('value', [float('nan'), float('inf'), -1])
def test_invalid_measurements_fail_closed(value):
    item = rows(1)[0]; item['energy_wh']['total'] = value
    with pytest.raises(Exception) as error:
        compact([item])
    assert error.value.status_code == 502


@pytest.fixture
def client(monkeypatch):
    app.dependency_overrides[get_current_user] = lambda: AuthUserRecord(username='teacher', role='teacher', school_ids=('school_10',))
    devices=[dict(id='plug-1', type='plug', label='Plug', room_id='teachers'),
             dict(id='meter', type='three_phase_meter', label='Meter', room_id='all_rooms',
                  phases={'a': dict(target_room_id='teachers', display_label='Lighting'),
                          'b': dict(target_room_id='teachers', display_label='AC'),
                          'c': dict(target_room_id='other', display_label='Other')})]
    monkeypatch.setattr(service, 'list_school_room_energy_devices', lambda *args: devices)
    monkeypatch.setattr(service, 'uses_school_wide_all_rooms_aggregate', lambda school, room: room=='all_rooms')
    monkeypatch.setattr(service, 'fetch_shelly_hourly_energy', lambda *args, **kwargs: dict(items=rows()))
    try:
        with TestClient(app) as client:
            yield client
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_additive_route_default_acl_and_whole_school(client):
    url='/energy/schools/school_10/rooms/hourly-energy'
    params=dict(room_key='teachers', start='2026-09-01T00:00:00Z', end='2026-09-02T00:00:00Z')
    legacy=client.get(url,params=params)
    explicit=client.get(url,params={**params,'format':'legacy'})
    response=client.get(url,params={**params,'format':'compact'})
    assert response.status_code == legacy.status_code == 200
    assert legacy.json() == explicit.json()
    assert 'contract' not in legacy.json() and 'summary' not in legacy.json()
    assert 'by_device' in legacy.json()['points'][0]
    assert response.json()['contract'] == 'consumption.compact.v1'
    assert 'by_device' not in response.json()['points'][0]
    assert response.json()['points'][0]['energy_wh_total'] == 300
    whole=client.get(url,params={**params,'format':'compact','room_key':'all_rooms'})
    assert whole.json()['points'][0]['energy_wh_total'] == 9000 # whole meter total, not sum of phases + total
    assert client.get(url,params={**params,'format':'bad'}).status_code == 422
    assert client.get(url.replace('school_10','school_11'),params={**params,'format':'compact'}).status_code == 403
    assert client.get(url,params={**params,'format':'compact','end':'2027-09-01T00:00:00Z'}).status_code == 400
    app.dependency_overrides.pop(get_current_user)
    assert client.get(url,params={**params,'format':'compact'}).status_code == 401


def test_null_is_preserved_through_canonical_query_only_for_compact(monkeypatch):
    import main
    from contextlib import contextmanager
    from monitoring.local_data import local_read
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, params):
            assert 'SELECT' in sql and 'shelly_plug_hourly_energy' in sql
        def fetchall(self):
            return [dict(device_id='shellyplug-fixture', window_start=START, window_end=START+timedelta(hours=1),
                         energy_wh=None, is_working_day=1, is_working_hour=1, created_at=START)]
    class Connection:
        def cursor(self): return Cursor()
    @contextmanager
    def connection(): yield Connection()
    monkeypatch.setattr(main, 'get_connection', connection)
    params=[('device_id','shellyplug-fixture'),('start','2026-09-01T00:00'),('end','2026-09-02T00:00')]
    legacy=local_read('/shelly/hourly-energy',params)
    preserved=local_read('/shelly/hourly-energy',params+[('preserve_missing',True)])
    assert legacy['items'][0]['energy_wh']['total'] == 0
    assert preserved['items'][0]['energy_wh']['total'] is None
    result=compact(preserved['items'],plan=[('shellyplug-fixture',None,'Plug')])
    assert result['count'] == 0
    assert result['summary']['total_energy_kwh'] is None


def test_compact_is_compatible_with_naive_and_aware_internal_windows():
    from datetime import timezone
    naive=compact(rows(24))
    aware=compact(rows(24),start=START.replace(tzinfo=timezone.utc),end=(START+timedelta(hours=24)).replace(tzinfo=timezone.utc))
    assert naive == aware
    assert all(p['window_start'].endswith('Z') and '+00:00Z' not in p['window_start'] for p in aware['points'])
