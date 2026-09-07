"""Counter semantics and conservative hourly accounting; no network required."""
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


parser = load('shelly_counter_parser', 'shelly-ingestor/counters.py')
ce = load('counter_energy', 'energy-aggregator/counter_energy.py')
agg = load('shelly_counter_aggregator', 'energy-aggregator/main.py')
START = datetime(2026, 9, 7, 5, tzinfo=timezone.utc)


def samples(*, offset=0, start=START, count=61, kind='import', returned=None):
    return [ce.Sample(start+timedelta(minutes=i, seconds=offset), 1000.0+i,
                      returned, kind) for i in range(count)]


def energy(rows, start=START):
    return ce.hourly_energy(rows, start, start+ce.HOUR)


def test_pro3em_extracts_per_phase_wh_without_total_double_counting():
    payload = {'a_total_act_energy': 11, 'b_total_act_energy': 22,
               'c_total_act_energy': 33, 'total_act': 66,
               'a_total_act_ret_energy': 5}
    rows = parser.counter_rows('shellypro3em-test', 'shellypro3em-test/status/emdata:0', payload, START)
    assert [r[1] for r in rows] == ['a', 'b', 'c']
    assert [r[3] for r in rows] == [11, 22, 33]
    assert rows[0][4:] == (5, 'import', 'emdata:0')


def test_plug_counter_uses_receipt_time_not_minute_timestamp_or_mwh():
    payload = {'aenergy': {'total': 110, 'minute_ts': 1, 'by_minute': [9000]},
               'ret_aenergy': {'total': 10}}
    row, = parser.counter_rows('shellyplugsg3-test', 'shellyplugsg3-test/status/switch:0', payload, START)
    assert row[2:6] == (START, 110, 10, 'absolute')


@pytest.mark.parametrize('bad', [True, -1, float('nan'), float('inf'), '1', None, 10**1000])
def test_bad_counter_is_not_ingested(bad):
    assert parser.counter_rows('shellypro3em-test', 'shellypro3em-test/status/emdata:0', {'a_total_act_energy': bad}, START) == []


@pytest.mark.parametrize('payload', [None, [], {'errors': ['database_error'], 'a_total_act_energy': 1}])
def test_invalid_payload_or_meter_error_is_not_a_counter(payload):
    assert parser.counter_rows('shellypro3em-test', 'shellypro3em-test/status/emdata:0', payload, START) == []


def test_power_message_is_not_a_counter_and_wrong_topic_is_rejected():
    assert not parser.counter_rows('shellypro3em-test', 'shellypro3em-test/status/em:0', {'a_act_power': 9000}, START)
    assert not parser.counter_rows('shellypro3em-test', 'other/status/emdata:0', {'a_total_act_energy': 1}, START)


@pytest.mark.parametrize('offset', [-29, 0, 1.5, 29])
def test_boundary_receipt_jitter_preserves_delta(offset):
    result = energy(samples(offset=offset))
    assert result.energy_wh == 60 and result.reason == 'observed'
    assert result.start_offset_seconds == offset


def test_adjacent_hours_share_boundary_and_conserve_total():
    rows = samples(offset=1.3, count=121)
    assert sum(energy(rows, START+i*ce.HOUR).energy_wh for i in range(2)) == 120


def test_ten_minute_gap_preserves_counter_delta_when_boundaries_exist():
    rows = samples()
    del rows[20:30]
    result = energy(rows)
    assert result.energy_wh == 60 and result.reason == 'observed'
    assert result.max_gap_seconds == 660


def test_no_post_outage_spike_and_next_complete_hour_recovers():
    rows = samples(count=121)
    del rows[55:65]
    assert energy(rows).energy_wh is None
    assert energy(rows, START+ce.HOUR).energy_wh is None
    recovered = samples(start=START+2*ce.HOUR)
    assert energy(recovered, START+2*ce.HOUR).energy_wh == 60


def test_reset_hidden_by_positive_end_minus_start_is_detected():
    rows = samples()
    rows[30] = ce.Sample(rows[30].observed_at, 0, None, 'import')
    assert energy(rows).reason == 'counter_reset'
    assert energy(rows).energy_wh is None


def test_returned_counter_reset_is_detected_separately():
    rows = samples(returned=10)
    rows[30] = ce.Sample(rows[30].observed_at, 1030, 0, 'import')
    assert energy(rows).reason == 'counter_reset'


def test_plug_absolute_energy_excludes_returned_but_pro3em_import_does_not():
    rows = [ce.Sample(START+timedelta(minutes=i), 1000+2*i, 100+i, 'absolute') for i in range(61)]
    assert energy(rows).energy_wh == 60
    assert energy([ce.Sample(s.observed_at,s.energy_wh,s.returned_energy_wh,'import') for s in rows]).energy_wh == 120


def test_real_zero_is_distinct_from_missing():
    rows = [ce.Sample(s.observed_at, 10, None, 'import') for s in samples()]
    assert energy(rows).energy_wh == 0
    assert energy([]).energy_wh is None
    assert energy(rows[10:-10]).reason == 'missing_boundary'


def test_duplicates_are_idempotent_but_conflicts_are_missing():
    rows = samples()
    assert energy(rows+rows[::-1]).energy_wh == 60
    assert energy(rows+[ce.Sample(rows[20].observed_at, 999, None, 'import')]).reason == 'conflicting_timestamp'


@pytest.mark.parametrize('value', [float('nan'), float('inf'), -2, True])
def test_invalid_stored_values_cannot_be_counted(value):
    rows = samples()
    rows[30] = ce.Sample(rows[30].observed_at, value, None, 'import')
    assert energy(rows).reason == 'invalid_counter'


def test_counter_shape_cannot_change_mid_hour():
    rows = samples()
    rows[30] = ce.Sample(rows[30].observed_at, 1030, 0, 'absolute')
    assert energy(rows).reason == 'counter_shape_changed'


def test_short_gap_can_use_counter_delta_without_power_interpolation():
    rows = samples()
    del rows[25:27]
    assert energy(rows).energy_wh == 60


@pytest.mark.parametrize('stamp', ['2026-03-29T00:00:00Z', '2026-10-25T00:00:00Z', '2026-10-25T01:00:00Z'])
def test_dst_uses_distinct_elapsed_hours(stamp):
    start = agg.parse_hour(stamp)
    assert energy(samples(start=start),start).energy_wh == 60


def test_naive_bounds_and_partial_hours_are_rejected():
    with pytest.raises(ValueError): agg.parse_hour('2026-09-07T08:00:00')
    with pytest.raises(ValueError): agg.parse_hour('2026-09-07T08:01:00+03:00')
    with pytest.raises(ValueError): ce.hourly_energy([], START, START+timedelta(minutes=30))


def test_cli_refuses_writes_without_cutover(monkeypatch):
    monkeypatch.delenv('SHELLY_COUNTER_START', raising=False)
    monkeypatch.setattr(sys, 'argv', ['aggregator'])
    with pytest.raises(SystemExit) as exc: agg.main()
    assert exc.value.code == 2


def test_cli_refuses_historical_rewrite_before_cutover(monkeypatch):
    monkeypatch.setenv('SHELLY_COUNTER_START','2026-09-07T00:00:00Z')
    monkeypatch.setattr(sys,'argv',['aggregator','--start','2026-09-01T00:00:00Z','--end','2026-09-01T01:00:00Z'])
    with pytest.raises(SystemExit) as exc: agg.main()
    assert exc.value.code == 2


def test_user_boundary_gap_case_and_sixty_second_limit():
    rows = [ce.Sample(START+timedelta(seconds=-5),100,None,'import'),
            ce.Sample(START+timedelta(minutes=4,seconds=15),101,None,'import'),
            ce.Sample(START+ce.HOUR+timedelta(seconds=3),102,None,'import')]
    assert energy(rows).energy_wh == 2
    assert energy(samples(offset=61)).reason == 'missing_boundary'


def test_sixty_second_boundary_tolerance_is_inclusive():
    rows=[ce.Sample(START-timedelta(seconds=60),100,None,'import'),
          ce.Sample(START+ce.HOUR+timedelta(seconds=60),102,None,'import')]
    assert energy(rows).energy_wh==2
