from datetime import datetime

from dateutil.tz import tzutc

from blurr.core.store_key import Key
from blurr.runner.local_runner import LocalRunner


def test_only_stream_dtc_provided():
    local_runner = LocalRunner(['tests/data/raw.json'], 'tests/data/stream.yml', None)
    local_runner.execute()

    assert len(local_runner._block_data) == 8

    # Stream DTC output
    assert local_runner._block_data[Key('userA', 'session', datetime(2018, 3, 7, 23, 35, 31))] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
        '_end_time': datetime(2018, 3, 7, 23, 35, 32, tzinfo=tzutc()).isoformat(),
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }

    assert local_runner._block_data[Key('userA', 'session', datetime(2018, 3, 7, 22, 35, 31))] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()).isoformat(),
        '_end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()).isoformat(),
        'events': 1,
        'country': 'US',
        'continent': 'North America'
    }

    assert local_runner._block_data[Key('userA', 'state')] == {
        '_identity': 'userA',
        'country': 'IN',
        'continent': 'World'
    }

    assert local_runner._block_data[Key('userB', 'session', datetime(2018, 3, 7, 23, 35, 31))] == {
        '_identity': 'userB',
        '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
        '_end_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()).isoformat(),
        'events': 1,
        'country': '',
        'continent': ''
    }

    assert not local_runner._window_data


def test_no_variable_aggreate_data_stored():
    local_runner = LocalRunner(['tests/data/raw.json'], 'tests/data/stream.yml', None)
    local_runner.execute()

    # Variables should not be stored
    assert Key('userA', 'vars') not in local_runner._block_data


def test_stream_and_window_dtc_provided():
    local_runner = LocalRunner(['tests/data/raw.json'], 'tests/data/stream.yml',
                               'tests/data/window.yml')
    local_runner.execute()

    assert not local_runner._block_data

    # Window DTC output
    assert local_runner._window_data['userA'] == [{
        'last_session.events': 1,
        'last_session._identity': 'userA',
        'last_day.total_events': 1,
        'last_day._identity': 'userA'
    }]
    assert local_runner._window_data['userB'] == []


def test_write_output_file_only_source_dtc_provided(tmpdir):
    local_runner = LocalRunner(['tests/data/raw.json'], 'tests/data/stream.yml', None)
    window_data = local_runner.execute()
    output_file = tmpdir.join('out.txt')
    local_runner.write_output_file(str(output_file), window_data)
    output_text = output_file.readlines(cr=False)
    assert ('["userA/session/2018-03-07T22:35:31+00:00", {'
            '"_identity": "userA", '
            '"_start_time": "2018-03-07T22:35:31+00:00", '
            '"_end_time": "2018-03-07T22:35:31+00:00", '
            '"events": 1, '
            '"country": "US", '
            '"continent": "North America"'
            '}]') in output_text


def test_write_output_file_with_stream_and_window_dtc_provided(tmpdir):
    local_runner = LocalRunner(['tests/data/raw.json'], 'tests/data/stream.yml',
                               'tests/data/window.yml')
    window_data = local_runner.execute()
    output_file = tmpdir.join('out.txt')
    local_runner.write_output_file(str(output_file), window_data)
    output_text = output_file.readlines(cr=False)
    assert 'last_day._identity,last_day.total_events,last_session._identity,last_session.events' in output_text
    assert 'userA,1,userA,1' in output_text
