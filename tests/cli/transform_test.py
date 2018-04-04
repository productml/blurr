import json
from typing import Any, Optional

from blurr.cli.cli import cli
from blurr.cli.out import Out
from blurr.cli.transform import transform


def run_command(stream_dtc_file: Optional[str], window_dtc_file: Optional[str],
                source: Optional[str], raw_json_files: Optional[str], out: Out) -> int:
    return cli({
        'transform': True,
        'validate': False,
        '--streaming-dtc': stream_dtc_file,
        '--window-dtc': window_dtc_file,
        '--source': source,
        '<raw-json-files>': raw_json_files,
    }, out)


def assert_record_in_ouput(record: Any, out: str) -> None:
    assert json.dumps(record) in out


def test_transform_invalid(capsys) -> None:
    assert run_command(None, None, None, None, Out()) == 1
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ('Streaming DTC file not provided and could not be found in '
                   'the current directory.\n')


def test_transform_only_window(capsys) -> None:
    assert run_command(None, 'tests/data/window.yml', None, None, Out()) == 1
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ('Streaming DTC file not provided and could not be found in '
                   'the current directory.\n')


def test_transform_no_raw_data(capsys) -> None:
    assert run_command('tests/data/stream.yml', None, None, None, Out()) == 0
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ''


def test_transform_only_stream(capsys) -> None:
    assert run_command('tests/data/stream.yml', None, 'tests/data/raw.json', None, Out()) == 0
    out, err = capsys.readouterr()
    assert_record_in_ouput(('userA/session/2018-03-07T22:35:31+00:00', {
        'identity': 'userA',
        'start_time': '2018-03-07 22:35:31+00:00',
        'end_time': '2018-03-07 22:35:31+00:00',
        'events': 1,
        'country': 'US',
        'continent': 'North America'
    }), out)
    assert_record_in_ouput(('userA/state', {
        'identity': 'userA',
        'country': 'IN',
        'continent': 'World'
    }), out)
    assert_record_in_ouput(('userA/session', {
        'identity': 'userA',
        'start_time': '2018-03-07 23:35:31+00:00',
        'end_time': '2018-03-07 23:35:32+00:00',
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }), out)
    assert err == ''


def test_transform_valid_raw_with_source(capsys) -> None:
    assert run_command('tests/data/stream.yml', 'tests/data/window.yml',
                       'tests/data/raw.json,tests/data/raw.json', None, Out()) == 0
    out, err = capsys.readouterr()
    assert_record_in_ouput(('userA', [{
        'last_session.identity': 'userA',
        'last_session.events': 2,
        'last_day.identity': 'userA',
        'last_day.total_events': 2
    }]), out)
    assert err == ''


def test_transform_valid_raw_without_source(capsys) -> None:
    assert run_command('tests/data/stream.yml', 'tests/data/window.yml', None,
                       'tests/data/raw.json,tests/data/raw.json', Out()) == 0
    out, err = capsys.readouterr()
    assert_record_in_ouput(('userA', [{
        'last_session.identity': 'userA',
        'last_session.events': 2,
        'last_day.identity': 'userA',
        'last_day.total_events': 2
    }]), out)
    assert err == ''
