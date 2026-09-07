"""Real PostgreSQL tests; explicitly restricted to a disposable local database."""
import os
from datetime import timedelta
from pathlib import Path

import psycopg2
import pytest

from test_shelly_counter_contract import ROOT, START, parser, ce, agg, samples

DSN = os.getenv('COUNTER_TEST_DSN')
pytestmark = pytest.mark.skipif(not DSN, reason='Requires isolated local PostgreSQL')


@pytest.fixture()
def db():
    assert 'host=127.0.0.1' in DSN and 'local-fixture-only' in DSN
    conn = psycopg2.connect(DSN)
    with conn:
        with conn.cursor() as cur:
            cur.execute('''DROP SCHEMA IF EXISTS counter_fixture CASCADE; CREATE SCHEMA counter_fixture;
                SET search_path TO counter_fixture;
                CREATE TABLE shelly_devices(device_id TEXT PRIMARY KEY);
                CREATE TABLE shelly_plug_hourly_energy(device_id TEXT,window_start TIMESTAMPTZ,window_end TIMESTAMPTZ,
                    energy_wh float8,is_working_day int,is_working_hour int,created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY(device_id,window_start,window_end));
                CREATE TABLE shelly_pro3em_hourly_energy(device_id TEXT,window_start TIMESTAMPTZ,window_end TIMESTAMPTZ,
                    a_energy_wh float8,b_energy_wh float8,c_energy_wh float8,total_energy_wh float8,
                    is_working_day int,is_working_hour int,created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY(device_id,window_start,window_end));
            ''')
    with conn.cursor() as cur:
        cur.execute((ROOT/'db/migrations/015_shelly_energy_counters.sql').read_text())
    yield conn
    conn.close()


def seed(db, device='shellypro3em-test', skip_b=(), count=61):
    with db:
        with db.cursor() as cur:
            cur.execute('INSERT INTO shelly_devices VALUES (%s)',(device,))
        for i in range(count):
            ts = START+timedelta(minutes=i,seconds=1)
            if device.startswith('shellypro3em'):
                payload = {f'{p}_total_act_energy':1000+(j+1)*i for j,p in enumerate('abc') if not (p=='b' and i in skip_b)}
                topic = f'{device}/status/emdata:0'
            else:
                payload = {'aenergy': {'total':1000+2*i,'minute_ts':1}, 'ret_aenergy':{'total':100+i}}
                topic = f'{device}/status/switch:0'
            parser.insert_counters(db,device,topic,payload,ts)


def test_parser_to_hourly_phase_nulls_without_diagnostic_rows(db):
    seed(db,skip_b=range(0,5))
    counts=agg.aggregate(db,START,START+ce.HOUR)
    assert counts=={'observed':2,'missing_boundary':1}
    with db.cursor() as cur:
        cur.execute('SELECT a_energy_wh,b_energy_wh,c_energy_wh,total_energy_wh FROM shelly_pro3em_hourly_energy')
        assert cur.fetchone()==(60,None,180,None)
        cur.execute("SELECT to_regclass('shelly_energy_hourly_quality')")
        assert cur.fetchone()==(None,)


def test_plug_receipt_time_returned_energy_and_idempotent_replay(db):
    seed(db,'shellyplugsg3-test')
    for _ in range(2): agg.aggregate(db,START,START+ce.HOUR)
    with db.cursor() as cur:
        cur.execute('SELECT energy_wh FROM shelly_plug_hourly_energy')
        assert cur.fetchall()==[(60,)]
        cur.execute('SELECT timestamp_basis,min(observed_at) FROM shelly_energy_counters GROUP BY timestamp_basis')
        assert cur.fetchone()==('received_at',START+timedelta(seconds=1))


def test_late_arrival_can_repair_missing_without_accumulating_twice(db):
    seed(db,skip_b=range(0,5))
    agg.aggregate(db,START,START+ce.HOUR)
    with db:
        for i in range(0,5):
            parser.insert_counters(db,'shellypro3em-test','shellypro3em-test/status/emdata:0',
                {'b_total_act_energy':1000+2*i},START+timedelta(minutes=i,seconds=1))
    agg.aggregate(db,START,START+ce.HOUR)
    with db.cursor() as cur:
        cur.execute('SELECT total_energy_wh FROM shelly_pro3em_hourly_energy')
        assert cur.fetchone()==(360,)


def test_missing_replay_removes_stale_hour_without_zero_or_metadata(db):
    seed(db)
    agg.aggregate(db,START,START+ce.HOUR)
    with db:
        with db.cursor() as cur: cur.execute('DELETE FROM shelly_energy_counters')
    agg.aggregate(db,START,START+ce.HOUR)
    with db.cursor() as cur:
        cur.execute('SELECT count(*) FROM shelly_pro3em_hourly_energy')
        assert cur.fetchone()==(0,)


def test_readonly_preview_does_not_write(db):
    seed(db)
    assert agg.aggregate(db,START,START+ce.HOUR,dry_run=True)=={'observed':3}
    with db.cursor() as cur:
        cur.execute('SHOW transaction_read_only')
        assert cur.fetchone()==('on',)
        cur.execute('SELECT count(*) FROM shelly_pro3em_hourly_energy')
        assert cur.fetchone()==(0,)


def test_migration_is_idempotent_and_preserves_existing_history(db):
    with db:
        with db.cursor() as cur:
            cur.execute("INSERT INTO shelly_plug_hourly_energy VALUES ('legacy',%s,%s,123,1,1,NOW())",(START,START+ce.HOUR))
    with db.cursor() as cur:
        cur.execute((ROOT/'db/migrations/015_shelly_energy_counters.sql').read_text())
        cur.execute('SELECT energy_wh FROM shelly_plug_hourly_energy')
        assert cur.fetchone()==(123,)


def test_postgres_refuses_nonfinite_counters(db):
    with pytest.raises(psycopg2.errors.CheckViolation):
        with db:
            with db.cursor() as cur:
                cur.execute("INSERT INTO shelly_energy_counters(device_id,channel,observed_at,energy_wh,counter_kind,source_component) VALUES ('bad','a',NOW(),'NaN','import','emdata:0')")
