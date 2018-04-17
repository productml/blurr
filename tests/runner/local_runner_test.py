from datetime import datetime

from dateutil.tz import tzutc

from blurr.core.store_key import Key
from blurr.runner.local_runner import LocalRunner


def test_local_runner_stream_only():
    local_runner = LocalRunner(['tests/data/raw.json'], 'tests/data/stream.yml', None)
    local_runner.execute()

    assert len(local_runner._block_data) == 7

    # Stream DTC output
    assert local_runner._block_data[Key('userA', 'session')] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 23, 35, 32, tzinfo=tzutc()),
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }

    assert local_runner._block_data[Key('userA', 'session',
                                        datetime(2018, 3, 7, 22, 35, 31))] == {
        '_identity': 'userA',
        '_start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        'events': 1,
        'country': 'US',
        'continent': 'North America'
    }  # yapf: disable

    assert local_runner._block_data[Key('userA', 'state')] == {
        '_identity': 'userA',
        'country': 'IN',
        'continent': 'World'
    }

    assert local_runner._block_data[Key('userB', 'session')] == {
        '_identity': 'userB',
        '_start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        '_end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        'events': 1,
        'country': '',
        'continent': ''
    }

    assert not local_runner._window_data


def test_local_runner_no_vars_stored():
    local_runner = LocalRunner(['tests/data/raw.json'], 'tests/data/stream.yml', None)
    local_runner.execute()

    # Variables should not be stored
    assert Key('userA', 'vars') not in local_runner._block_data


def test_local_runner_with_window():
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
