import json
import logging
from typing import Any, Optional

from blurr.cli.cli import cli


def run_command(stream_dtc_file: Optional[str], window_dtc_file: Optional[str],
                source: Optional[str], raw_json_files: Optional[str]) -> int:
    return cli({
        'transform': True,
        'validate': False,
        '--runner': None,
        '--streaming-dtc': stream_dtc_file,
        '--window-dtc': window_dtc_file,
        '--data-processor': None,
        '--source': source,
        '<raw-json-files>': raw_json_files,
    })


def assert_record_in_ouput(record: Any, out_text: str) -> None:
    assert json.dumps(record) in out_text


def test_transform_invalid(capsys) -> None:
    assert run_command(None, None, None, None) == 1
    out, err = capsys.readouterr()
    assert ('Streaming DTC file not provided and could not be found in '
            'the current directory.') in err


def test_transform_only_window(capsys) -> None:
    assert run_command(None, 'tests/data/window.yml', None, None) == 1
    out, err = capsys.readouterr()
    assert out == ''
    assert ('Streaming DTC file not provided and could not be found in '
            'the current directory.') in err


def test_transform_no_raw_data(capsys) -> None:
    assert run_command('tests/data/stream.yml', None, None, None) == 0
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ''


def test_transform_only_stream(capsys) -> None:
    assert run_command('tests/data/stream.yml', None, 'tests/data/raw.json', None) == 0
    out, err = capsys.readouterr()
    assert_record_in_ouput(('userA/session/2018-03-07T22:35:31+00:00', {
        '_identity': 'userA',
        '_start_time': '2018-03-07 22:35:31+00:00',
        '_end_time': '2018-03-07 22:35:31+00:00',
        'events': 1,
        'country': 'US',
        'continent': 'North America'
    }), out)
    assert_record_in_ouput(('userA/state', {
        '_identity': 'userA',
        'country': 'IN',
        'continent': 'World'
    }), out)
    assert_record_in_ouput(('userA/session', {
        '_identity': 'userA',
        '_start_time': '2018-03-07 23:35:31+00:00',
        '_end_time': '2018-03-07 23:35:32+00:00',
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }), out)
    assert err == ''


def test_transform_valid_raw_with_source(capsys) -> None:
    assert run_command('tests/data/stream.yml', 'tests/data/window.yml',
                       'tests/data/raw.json,tests/data/raw.json', None) == 0
    out, err = capsys.readouterr()
    assert_record_in_ouput(('userA', [{
        'last_session._identity': 'userA',
        'last_session.events': 2,
        'last_day._identity': 'userA',
        'last_day.total_events': 2
    }]), out)
    assert err == ''


def test_transform_valid_raw_without_source(capsys) -> None:
    assert run_command('tests/data/stream.yml', 'tests/data/window.yml', None,
                       'tests/data/raw.json,tests/data/raw.json') == 0
    out, err = capsys.readouterr()
    assert_record_in_ouput(('userA', [{
        'last_session._identity': 'userA',
        'last_session.events': 2,
        'last_day._identity': 'userA',
        'last_day.total_events': 2
    }]), out)
    assert err == ''
