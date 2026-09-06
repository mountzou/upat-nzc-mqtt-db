"""Integration against a disposable LOCAL PostgreSQL; never production."""
import base64
import hashlib
import os
from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import patch

import jwt
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import pytest
from fastapi.testclient import TestClient
import main
from auth_service import AuthVerifyRateLimiter
from monitoring.services.energy_production import aggregates as pv

DSN=os.getenv('MONITORING_TEST_DSN')
pytestmark=pytest.mark.skipif(not DSN,reason='Requires disposable local PostgreSQL')

@pytest.fixture()
def database(monkeypatch):
    assert DSN and 'host=127.0.0.1' in DSN and 'local-fixture-only' in DSN, 'Refuse non-local fixture database'
    def connect():
        conn=psycopg2.connect(DSN,cursor_factory=RealDictCursor)
        with conn.cursor() as cur: cur.execute('SET search_path TO monitoring_fixture')
        return conn
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute('''DROP SCHEMA IF EXISTS monitoring_fixture CASCADE; CREATE SCHEMA monitoring_fixture;
                SET search_path TO monitoring_fixture;
                CREATE TABLE pv_plants(id INTEGER PRIMARY KEY,site_key TEXT);
                INSERT INTO pv_plants VALUES (1,'upat-pv');
                CREATE TABLE pv_plant_readings_5m(plant_id INTEGER, observed_at TIMESTAMPTZ,local_date DATE,
                  active_power_kw DOUBLE PRECISION,quality_status TEXT,PRIMARY KEY(plant_id,observed_at));
                CREATE TABLE app_users(username TEXT PRIMARY KEY,password_hash TEXT,role TEXT,school_id TEXT,
                  municipality_id TEXT,school_ids TEXT[],is_active BOOLEAN,token_version INTEGER,theme TEXT,
                  onboarding_completed TEXT[],last_login_at TIMESTAMPTZ,updated_at TIMESTAMPTZ);
                CREATE TABLE shelly_measurements(device_id TEXT, metric TEXT, value DOUBLE PRECISION, event_time TIMESTAMP);
                CREATE TABLE shelly_plug_hourly_energy(device_id TEXT,window_start TIMESTAMP,window_end TIMESTAMP,
                  energy_wh DOUBLE PRECISION,is_working_day INTEGER,is_working_hour INTEGER,created_at TIMESTAMP);
                CREATE TABLE shelly_pro3em_hourly_energy(device_id TEXT,window_start TIMESTAMP,window_end TIMESTAMP,
                  a_energy_wh DOUBLE PRECISION,b_energy_wh DOUBLE PRECISION,c_energy_wh DOUBLE PRECISION,
                  total_energy_wh DOUBLE PRECISION,is_working_day INTEGER,is_working_hour INTEGER,created_at TIMESTAMP);
            ''')
            salt=b'isolated-fixture-salt'; digest=hashlib.pbkdf2_hmac('sha256',b'fixture-password',salt,100_000)
            enc=lambda b:base64.urlsafe_b64encode(b).decode().rstrip('=')
            password_hash=f'pbkdf2_sha256$100000${enc(salt)}${enc(digest)}'
            cur.execute("INSERT INTO app_users VALUES (%s,%s,'teacher','school_10',NULL,ARRAY['school_10'],TRUE,1,'light',ARRAY[]::TEXT[],NULL,NOW())",('fixture_teacher',password_hash))
    monkeypatch.setattr(main,'get_connection',connect)
    monkeypatch.setattr(main,'AUTH_VERIFY_RATE_LIMITER',AuthVerifyRateLimiter())
    monkeypatch.setenv('AUTH_TOKEN_SECRET','isolated-session-signing-secret-'+'x'*40)
    monkeypatch.setattr(main,'AUTH_SERVICE_TOKEN','isolated-internal-service-'+'y'*40)
    monkeypatch.setattr(pv,'solar_production_cache',pv.SolarProductionCache())
    return connect

@pytest.fixture()
def client(database): return TestClient(main.app)

def login(client):
    result=client.post('/auth/login',json={'username':'fixture_teacher','password':'fixture-password'})
    assert result.status_code==200,result.text
    assert 'password_hash' not in result.text and 'token_version' not in result.json()['user']
    return {'Authorization':'Bearer '+result.json()['access_token']}

def insert_day(connect,day,power=6,missing=()):
    start=datetime.combine(day,time.min,pv.APP_TIMEZONE).astimezone(timezone.utc)
    end=datetime.combine(day+timedelta(days=1),time.min,pv.APP_TIMEZONE).astimezone(timezone.utc)
    n=int((end-start).total_seconds()/300)
    with connect() as conn:
        with conn.cursor() as cur:
            execute_values(cur,'INSERT INTO pv_plant_readings_5m VALUES %s',
                [(1,start+timedelta(minutes=5*i),day,power,'complete') for i in range(n) if i not in missing])
    return n

@pytest.mark.parametrize('day,hours',[(date(2026,9,1),24),(date(2026,3,29),23),(date(2026,10,25),25)])
def test_real_sql_daily_hourly_dst(database,client,day,hours):
    insert_day(database,day)
    headers=login(client)
    url=f'/energy/production/solar/aggregates?start_date={day}&end_date={day}'
    daily=client.get(url+'&resolution=day',headers=headers)
    assert daily.status_code==200,daily.text
    assert daily.json()['days'][0]['hours']==[]
    assert daily.json()['summary']['total_energy_kwh']==6*hours
    assert daily.json()['summary']['productive_hours']==hours
    hourly=client.get(url+'&resolution=hour',headers=headers)
    assert hourly.status_code==200,hourly.text
    values=hourly.json()['days'][0]['hours']
    assert len(values)==hours and len({v['start'] for v in values})==hours
    assert all(v['mean_power_kw']==6 and v['energy_kwh']==6 for v in values)
    assert hourly.json()['days'][0]['expected_samples']==hours*12

def test_gap_zero_and_missing_preserved(database,client):
    insert_day(database,date(2026,9,1),missing=range(12))
    insert_day(database,date(2026,9,2),power=0)
    data=client.get('/energy/production/solar/aggregates?start_date=2026-09-01&end_date=2026-09-03&resolution=hour',headers=login(client)).json()
    a,b,c=data['days']
    assert a['quality']=='partial' and a['energy_kwh']==138 and a['hours'][0]['mean_power_kw'] is None
    assert b['quality']=='complete' and b['energy_kwh']==0
    assert c['quality']=='missing' and c['energy_kwh'] is None
    assert data['summary']['partial_day_count']==1 and data['summary']['observed_day_count']==2
    assert data['summary']['total_energy_kwh']==138

def test_auth_scope_revocation_and_internal_verifier(database,client):
    assert client.get('/energy/schools/school_10/devices').status_code==401
    headers=login(client)
    assert client.get('/auth/me',headers=headers).json()['school_ids']==['school_10']
    assert client.get('/schools',headers=headers).json()[0]['id']=='school_10'
    assert client.get('/rooms?school_id=school_10',headers=headers).status_code==200
    assert client.get('/energy/schools/school_3/devices',headers=headers).status_code==403
    assert client.get('/indoor_environment/schools/school_3/devices',headers=headers).status_code==403
    body={'access_token':headers['Authorization'].split(' ',1)[1]}
    assert client.post('/internal/auth/session',json=body).status_code==401
    verified=client.post('/internal/auth/session',json=body,headers={'Authorization':'Bearer '+main.AUTH_SERVICE_TOKEN})
    assert verified.status_code==200 and verified.json()['token_version']==1
    assert verified.headers['cache-control']=='no-store'
    changed=client.patch('/auth/preferences',json={'theme':'dark'},headers=headers)
    assert changed.status_code==200 and changed.json()['theme']=='dark'
    with database() as conn:
        with conn.cursor() as cur: cur.execute("UPDATE app_users SET token_version=2")
    assert client.get('/auth/me',headers=headers).status_code==401

def test_auth_redaction_and_no_signing_configuration(database,client,monkeypatch):
    result=client.post('/auth/login',json={'username':'','password':'private-fixture-password'})
    assert result.status_code==422 and 'private-fixture-password' not in result.text
    assert client.post('/auth/login',json={'username':'fixture_teacher','password':'wrong'}).status_code==401
    monkeypatch.delenv('AUTH_TOKEN_SECRET')
    assert client.post('/auth/login',json={'username':'fixture_teacher','password':'fixture-password'}).status_code==503

def test_hourly_cache_serves_daily_and_does_not_prefetch_hours(database,client):
    day=date(2026,9,1);insert_day(database,day)
    with patch.object(pv,'query_days',wraps=pv.query_days) as query:
        one=pv.get_solar_production(day,day,'day')
        pv.get_solar_production(day,day,'day');assert query.call_count==1
        assert one.days[0].hours==[]
        pv.get_solar_production(day,day,'hour');assert query.call_count==2
        pv.get_solar_production(day,day,'day');assert query.call_count==2

def test_local_multi_meter_read_keeps_all_ids(database):
    from monitoring.local_data import local_read
    with database() as conn:
        with conn.cursor() as cur:
            for device,value in [('shellyplug-a',100),('shellyplug-b',200)]:
                cur.execute("INSERT INTO shelly_plug_hourly_energy VALUES (%s,'2026-09-01T00:00','2026-09-01T01:00',%s,1,1,NOW())",(device,value))
    result=local_read('/shelly/hourly-energy',[('device_id','shellyplug-a'),('device_id','shellyplug-b'),('start','2026-09-01T00:00'),('end','2026-09-01T02:00')])
    assert result['device_ids']==['shellyplug-a','shellyplug-b'] and result['count']==2

def test_public_cors_allows_only_named_frontends(client):
    request={'Origin':'https://schoolheroz.com','Access-Control-Request-Method':'GET','Access-Control-Request-Headers':'authorization'}
    result=client.options('/energy/schools/school_10/devices',headers=request)
    assert result.status_code==200 and result.headers['access-control-allow-origin']=='https://schoolheroz.com'
    request['Origin']='https://untrusted.example'
    assert client.options('/energy/schools/school_10/devices',headers=request).status_code==400


def test_phase_estimate_uses_sql_and_clips_last_bucket(database):
    from monitoring.local_data import local_read
    with database() as conn:
        with conn.cursor() as cur:
            for phase, power in [('a',120),('b',240),('c',360)]:
                for minute in (0,15,30):
                    cur.execute("INSERT INTO shelly_measurements VALUES (%s,%s,%s,%s)",
                        ('shellypro3em-fixture',phase+'_act_power',power,datetime(2026,9,1,0,minute)))
    values=local_read('/shelly/device/shellypro3em-fixture/energy',
        {'start':'2026-09-01T00:00','end':'2026-09-01T00:45','bucket_minutes':30})
    assert values['bucket_minutes']==30
    assert values['energy_wh']=={'a':90,'b':180,'c':270,'total':540}


def test_municipality_and_admin_catalog_scope(database,client):
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO app_users SELECT 'fixture_municipality',password_hash,'municipality',NULL,'municipality_kalamata',ARRAY['school_10','school_3'],TRUE,1,theme,onboarding_completed,NULL,NOW() FROM app_users WHERE username='fixture_teacher'")
            cur.execute("INSERT INTO app_users SELECT 'fixture_admin',password_hash,'system_admin',NULL,NULL,ARRAY[]::TEXT[],TRUE,1,theme,onboarding_completed,NULL,NOW() FROM app_users WHERE username='fixture_teacher'")
    for username,expected in [('fixture_municipality',{'school_10','school_3'}),('fixture_admin',None)]:
        session=client.post('/auth/login',json={'username':username,'password':'fixture-password'})
        assert session.status_code==200
        headers={'Authorization':'Bearer '+session.json()['access_token']}
        schools=client.get('/schools',headers=headers)
        assert schools.status_code==200
        if expected:
            assert {s['id'] for s in schools.json()}==expected
            assert client.get('/rooms?school_id=school_22',headers=headers).status_code==403
        else:assert len(schools.json())>2
        assert client.get('/energy/production/solar/aggregates?start_date=2026-09-01&end_date=2026-09-01&resolution=day',headers=headers).status_code==200
