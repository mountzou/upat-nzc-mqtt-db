"""Strict public/home contracts against disposable PostgreSQL, never production."""
import json
from datetime import datetime,timezone
from unittest.mock import Mock
import pytest
from fastapi.testclient import TestClient
import main
from monitoring.routes import indoor_environment as route
from monitoring.routes.auth import get_current_user
from monitoring.services.authentication import AuthUserRecord
from monitoring.services.environment import home_overview
from monitoring.services.environment.history import contract
from monitoring.utils import api_datetime
from test_environmental_history_postgres import db,insert,rollup,DSN

pytestmark=pytest.mark.skipif(not DSN,reason='Requires isolated local PostgreSQL fixture')
NOW=datetime(2026,9,7,10,17,tzinfo=timezone.utc)
BASE='/indoor_environment/devices/fixture/history'
HOME='/indoor_environment/schools/school_10/home'

@pytest.fixture
def client(db,monkeypatch):
    monkeypatch.setitem(main.app.dependency_overrides,get_current_user,lambda:AuthUserRecord(username='fixture',role='teacher',school_id='school_10',school_ids=['school_10']))
    monkeypatch.setattr(route,'find_school_id_for_device',lambda _: 'school_10')
    monkeypatch.setattr(route,'list_devices_for_school',lambda _: [{'id':'fixture','room_id':'fixture_room'}])
    monkeypatch.setattr(api_datetime,'utc_now',lambda:NOW)
    monkeypatch.setattr(contract,'utc_now',lambda:NOW)
    home_overview.home_cache.clear()
    with TestClient(main.app) as result:yield result
    home_overview.home_cache.clear()

@pytest.mark.parametrize('stream',[False,True])
def test_home_weighted_completed_hours_with_live_and_stable_envelope(db,client,stream):
    insert(db,[(1,'2026-09-07T09:01Z',10,'temperature'),(2,'2026-09-07T09:06Z',30,'temperature'),(3,'2026-09-07T09:07Z',30,'temperature'),(4,'2026-09-07T09:08Z',30,'temperature'),(5,'2026-09-07T10:16:30Z',900,'temperature')]);rollup(db,5)
    public=client.get(BASE+'?window=24h&alignment=clock&limit=24')
    assert public.status_code==200,public.text
    reading=public.json()['items'][0]['measurements']['temperature'];assert reading=={'value':25,'unit':'C','sample_count':4}
    response=client.post(HOME,json={'room_ids':['fixture_room'],'refresh':True,'stream':stream});assert response.status_code==200,response.text
    if stream:
        updates=[json.loads(line) for line in response.text.splitlines()];parts={r['part']:r for u in updates for r in u['rooms']};assert set(parts)=={'live','history'}
        live=parts['live']['live'];history=parts['history']['history'];assert all(not r['errors'] for r in parts.values())
    else:
        room=response.json()['rooms'][0];assert not room['errors'];live=room['live'];history=room['history']
    assert set(history)=={'device_id','count','items'}
    assert history['count']==1 and history['items'][0]['event_time']=='2026-09-07T12:00:00+03:00'
    assert history['items'][0]['measurements']['temperature']=={'value':25,'unit':'C'}
    assert live['items'][0]['measurements']['temperature']['value']==900


def test_strict_home_empty_history_is_empty_not_failure(db,client):
    insert(db,[(1,'2026-09-07T10:16:30Z',30,'temperature')]);rollup(db,1)
    r=client.post(HOME,json={'room_ids':['fixture_room']});assert r.status_code==200,r.text
    room=r.json()['rooms'][0];assert room['history']['items']==[] and room['history']['count']==0 and room['errors']==[]

@pytest.mark.parametrize('query',['aggregate=avg','rolling_1h=true','rolling_24h_hourly=true','rolling_1h=false','bucket_unit=hour','bucket_size=1','bucket_minutes=60','window=1h&start=2026-09-01T00:00Z&end=2026-09-02T00:00Z','interval=1h&limit=1'])
def test_strict_retired_and_conflicting_controls_reject_before_db(client,monkeypatch,query):
    connection=Mock(side_effect=AssertionError('Rejected request queried DB'));monkeypatch.setattr(main,'get_connection',connection)
    assert client.get(BASE+'?'+query).status_code==422;connection.assert_not_called()


def test_strict_bare_and_limit_only_have_fixed_24h_clock_meaning(client):
    for query in ('','?limit=168','?interval=1h&limit=24'):
        r=client.get(BASE+query);assert r.status_code==200,r.text;d=r.json();assert r.headers['X-Environmental-History-Contract']=='canonical'
        assert d['start']=='2026-09-06T13:00:00+03:00' and d['end']=='2026-09-07T13:00:00+03:00'
        assert d['count']==0


def test_strict_retained_complete_rollups_and_missing_edges(db,client):
    insert(db,[(1,'2026-08-01T09:01Z',10,'temperature'),(2,'2026-08-01T09:02Z',30,'temperature')]);rollup(db,2)
    with db() as c,c.cursor() as q:q.execute('DELETE FROM upat_measurements')
    params={'start':'2026-08-01T09:00Z','end':'2026-08-01T10:00Z','alignment':'window','limit':1}
    r=client.get(BASE,params=params);assert r.status_code==200,r.text;assert r.json()['items'][0]['measurements']['temperature']['value']==20
    r=client.get(BASE,params={**params,'start':'2026-08-01T09:02Z'});assert r.status_code==422 and 'no longer retained' in r.text
