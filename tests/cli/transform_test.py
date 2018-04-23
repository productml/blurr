import json
from typing import Any, Optional

from pytest import mark

from blurr.cli.cli import cli


def run_command(stream_dtc_file: Optional[str],
                window_dtc_file: Optional[str],
                source: Optional[str],
                raw_json_files: Optional[str],
                runner: Optional[str] = None,
                data_processor: Optional[str] = None) -> int:
    return cli({
        'transform': True,
        'validate': False,
        '--runner': runner,
        '--streaming-dtc': stream_dtc_file,
        '--window-dtc': window_dtc_file,
        '--data-processor': data_processor,
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


@mark.parametrize("runner", ['local', 'spark'])
def test_transform_only_stream(capsys, runner) -> None:
    assert run_command(
        stream_dtc_file='tests/data/stream.yml',
        window_dtc_file=None,
        source='tests/data/raw.json',
        raw_json_files=None,
        runner=runner) == 0
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
        "_identity": "userA",
        "_start_time": "2018-03-07 22:35:31+00:00",
        "_end_time": "2018-03-07 23:35:32+00:00",
        "country": "IN",
        "continent": "World"
    }), out)
    assert_record_in_ouput(('userA/session/2018-03-07T23:35:31+00:00', {
        '_identity': 'userA',
        '_start_time': '2018-03-07 23:35:31+00:00',
        '_end_time': '2018-03-07 23:35:32+00:00',
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }), out)
    assert err == ''


@mark.parametrize("runner", ['local', 'spark'])
def test_transform_valid_raw_with_source(capsys, runner) -> None:
    assert run_command(
        stream_dtc_file='tests/data/stream.yml',
        window_dtc_file='tests/data/window.yml',
        source='tests/data/raw.json,tests/data/raw.json',
        raw_json_files=None,
        runner=runner) == 0
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


def test_transform_invalid_runner(capsys) -> None:
    assert run_command(
        stream_dtc_file='tests/data/stream.yml',
        window_dtc_file='tests/data/window.yml',
        source=None,
        raw_json_files='tests/data/raw.json,tests/data/raw.json',
        runner='incorrect') == 1
    out, err = capsys.readouterr()
    assert 'Unknown runner: \'incorrect\'. Possible values: [\'local\', \'spark\']' in err


def test_transform_invalid_data_processor(capsys) -> None:
    assert run_command(
        stream_dtc_file='tests/data/stream.yml',
        window_dtc_file='tests/data/window.yml',
        source=None,
        raw_json_files='tests/data/raw.json,tests/data/raw.json',
        data_processor='incorrect') == 1
    out, err = capsys.readouterr()
    assert 'Unknown data-processor: \'local\'. Possible values: [\'ipfix\', \'simple\']' in err
