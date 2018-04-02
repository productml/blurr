import json
from typing import Any

from blurr.cli.out import Out
from blurr.cli.transform import transform


def assert_record_in_ouput(record: Any, out: str) -> None:
    assert json.dumps(record) in out


def test_transform_invalid(capsys) -> None:
    assert transform(None, None, [], Out()) == 1
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ('Streaming DTC file not provided and could not be found in '
                   'the current directory.\n')


def test_transform_only_window(capsys) -> None:
    assert transform(None, 'tests/data/window.yml', [], Out()) == 1
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ('Streaming DTC file not provided and could not be found in '
                   'the current directory.\n')


def test_transform_no_raw_data(capsys) -> None:
    assert transform('tests/data/stream.yml', None, [], Out()) == 0
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ''


def test_transform_only_stream(capsys) -> None:
    assert transform('tests/data/stream.yml', None, ['tests/data/raw.json'],
                     Out()) == 0
    out, err = capsys.readouterr()
    assert_record_in_ouput(('userA/session/2018-03-07T22:35:31+00:00', {
        'start_time': '2018-03-07 22:35:31+00:00',
        'end_time': '2018-03-07 22:35:31+00:00',
        'events': 1,
        'country': 'US',
        'continent': 'North America'
    }), out)
    assert_record_in_ouput(('userA/state', {
        'country': 'IN',
        'continent': 'World'
    }), out)
    assert_record_in_ouput(('userA/session', {
        'start_time': '2018-03-07 23:35:31+00:00',
        'end_time': '2018-03-07 23:35:32+00:00',
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }), out)
    assert err == ''


def test_transform_valid(capsys) -> None:
    assert transform('tests/data/stream.yml', 'tests/data/window.yml',
                     ['tests/data/raw.json'], Out()) == 0
    out, err = capsys.readouterr()
    assert_record_in_ouput(('userA', [{
        'last_session.events': 1,
        'last_day.total_events': 1
    }]), out)
    assert err == ''
