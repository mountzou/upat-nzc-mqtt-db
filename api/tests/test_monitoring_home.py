from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Event, Lock
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from main import app
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord
from monitoring.schemas import DeviceHistoryResponse
from monitoring.services.environment import home_overview as home


def payload(device='a', value=24):
    return DeviceHistoryResponse(device_id=device, count=1, items=[dict(device_id=device,
        event_time=datetime.now(timezone.utc), measurements={'temperature': {'value': value, 'unit': '°C'}})])


def test_separate_ttl_and_stale_failure():
    now = [0]
    cache = home.HomeCache(clock=lambda: now[0])
    calls = []
    def load(): calls.append(1); return payload()
    for kind in ('live', 'history'): cache.get(('school', 'a', kind), load)
    now[0] = 61
    for kind in ('live', 'history'): cache.get(('school', 'a', kind), load)
    assert len(calls) == 3
    now[0] = 122
    def fail(): raise HTTPException(503)
    stale = cache.get(('school', 'a', 'live'), fail)
    assert stale.stale
    assert stale.fetched_at == cache.entries[('school', 'a', 'live')].fetched_at
    now[0] = 2000
    with pytest.raises(HTTPException): cache.get(('school', 'a', 'live'), fail)


def test_singleflight_and_capacity():
    cache = home.HomeCache(capacity=2)
    started, release = Event(), Event()
    calls = []
    def load():
        calls.append(1); started.set(); release.wait(2); return payload()
    with ThreadPoolExecutor(max_workers=5) as pool:
        first = pool.submit(cache.get, ('s', 'a', 'live'), load)
        assert started.wait(1)
        rest = [pool.submit(cache.get, ('s', 'a', 'live'), load) for _ in range(4)]
        release.set()
        assert all(f.result().data == first.result().data for f in rest)
    assert len(calls) == 1
    cache.get(('s2', 'a', 'live'), load)
    cache.get(('s3', 'a', 'live'), load)
    assert len(cache.entries) == 2


@pytest.fixture
def client():
    app.dependency_overrides[get_current_user] = lambda: AuthUserRecord(username='teacher', role='teacher', school_id='school_10', school_ids=['school_10'])
    home.home_cache.clear()
    with TestClient(app) as client: yield client
    app.dependency_overrides.pop(get_current_user, None)
    home.home_cache.clear()


CATALOG = [dict(id='a', room_id='r1'), dict(id='b', room_id='r2')]


def test_auth_and_room_validation_before_cache(client):
    with patch('monitoring.routes.indoor_environment.list_devices_for_school', return_value=CATALOG), patch.object(home, 'get_device_history') as upstream:
        assert client.post('/indoor_environment/schools/school_11/home', json={'room_ids':['r1']}).status_code == 403
        assert client.post('/indoor_environment/schools/school_10/home', json={'room_ids':['foreign-room']}).status_code == 404
        assert client.post('/indoor_environment/schools/school_10/home', json={'room_ids':[]}).status_code == 422
        assert client.post('/indoor_environment/schools/school_10/home', json={'room_ids':['x']*9}).status_code == 422
        upstream.assert_not_called()


def test_batch_cache_and_partial_failure(client):
    def live(device, **kw): return payload(device)
    def history(device):
        if device == 'b': raise HTTPException(504)
        return payload(device)
    with patch('monitoring.routes.indoor_environment.list_devices_for_school', return_value=CATALOG), patch.object(home, 'get_device_history', side_effect=live) as live_mock, patch.object(home, 'get_device_rolling_twenty_four_hour_hourly_series', side_effect=history) as history_mock:
        response = client.post('/indoor_environment/schools/school_10/home', json={'room_ids':['r1','r2','r1']})
        assert response.status_code == 200
        rooms = response.json()['rooms']
        assert len(rooms) == 2
        assert rooms[0]['history'] and rooms[0]['live']
        assert rooms[1]['live'] and rooms[1]['history'] is None
        assert rooms[1]['errors'] == ['history_unavailable']
        assert 'home;dur=' in response.headers['server-timing']
        client.post('/indoor_environment/schools/school_10/home', json={'room_ids':['r1']})
        assert live_mock.call_count == 2 and history_mock.call_count == 2
        client.post('/indoor_environment/schools/school_10/home', json={'room_ids':['r1'], 'refresh':True})
        assert live_mock.call_count == 3 and history_mock.call_count == 3


def test_invalid_device_not_cached():
    cache = home.HomeCache()
    with pytest.raises(HTTPException): cache.get(('s','a','live'), lambda: payload('b'))
    assert not cache.entries


def test_multi_sensor_history_matches_latest():
    def live(device, **kw):
        result = payload(device)
        result.items[0].event_time = datetime(2026,9,5,12 if device == 'a' else 13,tzinfo=timezone.utc)
        return result
    with patch.object(home, 'get_device_history', side_effect=live), patch.object(home, 'get_device_rolling_twenty_four_hour_hourly_series', side_effect=lambda device: payload(device)) as history:
        home.home_cache.clear()
        result = home.get_home_overview('s', ['r'], [dict(id='a',room_id='r'),dict(id='b',room_id='r')])
        assert result['rooms'][0]['device_id'] == 'b'
        history.assert_called_once_with('b')


def test_live_stream_does_not_wait_for_history():
    release = Event()
    def slow_history(device):
        assert release.wait(3)
        return payload(device)
    home.home_cache.clear()
    with patch.object(home, 'get_device_history', side_effect=lambda device, **kw: payload(device)), patch.object(home, 'get_device_rolling_twenty_four_hour_hourly_series', side_effect=slow_history):
        updates = home.iter_home_overview('s', {'r1': [dict(id='a')]})
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                first = pool.submit(next, updates).result(timeout=1)
            assert first['rooms'][0]['part'] == 'live'
            assert first['rooms'][0]['live'].device_id == 'a'
        finally:
            release.set()
        rest = list(updates)
        assert [u['rooms'][0]['part'] for u in rest] == ['history']


def test_stream_contract_and_authorization(client):
    import json
    with patch('monitoring.routes.indoor_environment.list_devices_for_school', return_value=CATALOG), patch.object(home, 'get_device_history', side_effect=lambda device, **kw: payload(device)), patch.object(home, 'get_device_rolling_twenty_four_hour_hourly_series', side_effect=lambda device: payload(device)):
        assert client.post('/indoor_environment/schools/school_11/home', json={'room_ids':['r1'], 'stream':True}).status_code == 403
        assert client.post('/indoor_environment/schools/school_10/home', json={'room_ids':['foreign'], 'stream':True}).status_code == 404
        result = client.post('/indoor_environment/schools/school_10/home', json={'room_ids':['r1','r2'], 'stream':True})
        assert result.headers['content-type'].startswith('application/x-ndjson')
        assert result.headers['x-accel-buffering'] == 'no'
        assert result.content.endswith(b'\n')
        parts = [json.loads(line)['rooms'][0] for line in result.iter_lines()]
        assert {(p['room_id'], p['part']) for p in parts} == {(r, k) for r in ['r1','r2'] for k in ['live','history']}


def test_equal_timestamps_keep_sensor_and_history_cache_stable():
    def live(device, **kw):
        result = payload(device)
        result.items[0].event_time = datetime(2026,9,5,12,tzinfo=timezone.utc)
        return result
    home.home_cache.clear()
    with patch.object(home, 'get_device_history', side_effect=live), patch.object(home, 'get_device_rolling_twenty_four_hour_hourly_series', side_effect=lambda device: payload(device)) as history:
        for ids in [('a','b'), ('b','a')] * 5:
            result = home.get_home_overview('s', ['r'], [dict(id=d,room_id='r') for d in ids])
            assert result['rooms'][0]['device_id'] == 'b'
        history.assert_called_once_with('b')
