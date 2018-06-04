from datetime import datetime
from typing import List, Tuple, Any, Optional, Dict

from dateutil.tz import tzutc

from blurr.core.store_key import Key, KeyType
from blurr.runner.local_runner import LocalRunner
from tests.core.conftest import assert_aggregate_snapshot_equals


def execute_runner(stream_bts_file: str,
                   window_bts_file: Optional[str],
                   local_json_files: List[str],
                   old_state: Optional[Dict[str, Dict]] = None) -> Tuple[LocalRunner, Any]:
    runner = LocalRunner(stream_bts_file, window_bts_file)
    return runner, runner.execute(
        runner.get_identity_records_from_json_files(local_json_files), old_state)


def test_only_stream_bts_provided():
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    assert len(data) == 3

    # Stream BTS output
    assert_aggregate_snapshot_equals(data['userA'][0][Key(
        KeyType.TIMESTAMP, 'userA', 'session', [], datetime(2018, 3, 7, 23, 35, 31))], {
            '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
            '_end_time': datetime(2018, 3, 7, 23, 35, 32, tzinfo=tzutc()).isoformat(),
            'events': 2,
            'country': 'IN',
            'continent': 'World'
        })

    assert_aggregate_snapshot_equals(data['userA'][0][Key(
        KeyType.TIMESTAMP, 'userA', 'session', [], datetime(2018, 3, 7, 22, 35, 31))], {
            '_start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()).isoformat(),
            '_end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()).isoformat(),
            'events': 1,
            'country': 'US',
            'continent': 'North America'
        })

    assert_aggregate_snapshot_equals(data['userA'][0][Key(KeyType.DIMENSION, 'userA', 'state')], {
        'country': 'IN',
        'continent': 'World'
    })

    assert_aggregate_snapshot_equals(data['userB'][0][Key(
        KeyType.TIMESTAMP, 'userB', 'session', [], datetime(2018, 3, 7, 23, 35, 31))], {
            '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
            '_end_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
            'events': 1,
            'country': '',
            'continent': ''
        })

    for (_, window_data) in runner._per_user_data.values():
        assert window_data == []


def test_no_variable_aggreate_data_stored():
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])

    # Variables should not be stored
    assert Key(KeyType.DIMENSION, 'userA', 'vars') not in data


def test_stream_and_window_bts_provided():
    runner, data = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                  ['tests/data/raw.json'])

    # Window BTS output
    assert data['userA'][1] == [{
        'last_session.events': 1,
        'last_day.total_events': 1,
    }]
    assert data['userB'][1] == []


def test_stream_bts_with_state():
    _, data_combined = execute_runner('tests/data/stream.yml', None,
                                      ['tests/data/raw.json', 'tests/data/raw2.json'], None)

    _, data_separate = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'], None)
    old_state = {
        identity: block_data
        for identity, (block_data, window_data) in data_separate.items()
    }
    _, data_separate = execute_runner('tests/data/stream.yml', None, ['tests/data/raw2.json'],
                                      old_state)

    assert data_separate == data_combined


def test_stream_and_window_bts_with_state():
    _, data_combined = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                      ['tests/data/raw.json', 'tests/data/raw2.json'], None)

    _, data_separate = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                      ['tests/data/raw.json'], None)
    old_state = {
        identity: block_data
        for identity, (block_data, window_data) in data_separate.items()
    }
    _, data_separate = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                      ['tests/data/raw2.json'], old_state)

    assert data_separate == data_combined


def test_write_output_file_only_source_bts_provided(tmpdir):
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    output_file = tmpdir.join('out.txt')
    runner.write_output_file(str(output_file), data)
    output_text = output_file.readlines(cr=False)
    assert ('["userA/session//2018-03-07T22:35:31+00:00", {'
            '"_start_time": "2018-03-07T22:35:31+00:00", '
            '"_end_time": "2018-03-07T22:35:31+00:00", '
            '"events": 1, '
            '"country": "US", '
            '"continent": "North America"'
            '}]') in output_text


def test_write_output_file_with_stream_and_window_bts_provided(tmpdir):
    runner, data = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                  ['tests/data/raw.json'])
    output_file = tmpdir.join('out.txt')
    runner.write_output_file(str(output_file), data)
    output_text = output_file.readlines(cr=False)
    assert 'last_day.total_events,last_session.events' in output_text
    assert '1,1' in output_text
