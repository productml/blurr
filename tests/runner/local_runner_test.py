from datetime import datetime
from typing import List, Tuple, Any, Optional

from dateutil.tz import tzutc

from blurr.core.store_key import Key
from blurr.runner.local_runner import LocalRunner


def execute_runner(stream_dtc_file: str, window_dtc_file: Optional[str],
                   local_json_files: List[str]) -> Tuple[LocalRunner, Any]:
    runner = LocalRunner(stream_dtc_file, window_dtc_file)
    return runner, runner.execute(runner.get_identity_records_from_json_files(local_json_files))


def test_only_stream_dtc_provided():
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    assert len(data) == 8

    # Stream DTC output
    assert data[Key('userA', 'session', datetime(2018, 3, 7, 23, 35, 31))] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 23, 35, 32, tzinfo=tzutc()),
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }

    assert data[Key('userA', 'session', datetime(2018, 3, 7, 22, 35, 31))] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        'events': 1,
        'country': 'US',
        'continent': 'North America'
    }

    assert data[Key('userA', 'state')] == {
        '_identity': 'userA',
        'country': 'IN',
        'continent': 'World'
    }

    assert data[Key('userB', 'session', datetime(2018, 3, 7, 23, 35, 31))] == {
        '_identity': 'userB',
        '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()),
        'events': 1,
        'country': '',
        'continent': ''
    }

    assert not runner._window_data


def test_no_variable_aggreate_data_stored():
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])

    # Variables should not be stored
    assert Key('userA', 'vars') not in data


def test_stream_and_window_dtc_provided():
    runner, data = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                  ['tests/data/raw.json'])

    assert not runner._block_data

    # Window DTC output
    assert data['userA'] == [{
        'last_session.events': 1,
        'last_session._identity': 'userA',
        'last_day.total_events': 1,
        'last_day._identity': 'userA'
    }]
    assert data['userB'] == []


def test_write_output_file_only_source_dtc_provided(tmpdir):
    runner, data = execute_runner('tests/data/stream.yml', None, ['tests/data/raw.json'])
    output_file = tmpdir.join('out.txt')
    runner.write_output_file(str(output_file), data)
    output_text = output_file.readlines(cr=False)
    assert ('["userA/session/2018-03-07T22:35:31+00:00", {'
            '"_identity": "userA", '
            '"_start_time": "2018-03-07 22:35:31+00:00", '
            '"_end_time": "2018-03-07 22:35:31+00:00", '
            '"events": 1, '
            '"country": "US", '
            '"continent": "North America"'
            '}]') in output_text


def test_write_output_file_with_stream_and_window_dtc_provided(tmpdir):
    runner, data = execute_runner('tests/data/stream.yml', 'tests/data/window.yml',
                                  ['tests/data/raw.json'])
    output_file = tmpdir.join('out.txt')
    runner.write_output_file(str(output_file), data)
    output_text = output_file.readlines(cr=False)
    assert 'last_day._identity,last_day.total_events,last_session._identity,last_session.events' in output_text
    assert 'userA,1,userA,1' in output_text
