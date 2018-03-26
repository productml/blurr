from dateutil.tz import tzutc

from blurr.core.store import Key
from blurr.runner.local_runner import LocalRunner
from datetime import datetime, timezone


def test_local_runner():
    local_runner = LocalRunner(['tests/data/raw.json'],
                               'tests/data/stream.yml',
                               'tests/data/window.yml')
    local_runner.execute()

    assert len(local_runner._session_data) == 7

    # Stream DTC output
    assert local_runner._session_data[Key('userA', 'session')] == {
        'start_time': datetime(2018, 3, 7, 23, 35, 31, tzinfo=tzutc()),
        'end_time': datetime(2018, 3, 7, 23, 35, 32, tzinfo=tzutc()),
        'events': 2,
        'country': 'IN',
        'continent': 'World'
    }

    assert local_runner._session_data[Key(
        'userA', 'session', datetime(2018, 3, 7, 22, 35, 31))] == {
            'start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
            'end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
            'events': 1,
            'country': 'US',
            'continent': 'North America'
        }

    assert local_runner._session_data[Key('userA', 'state')] == {
        'country': 'IN',
        'continent': 'World'
    }

    assert local_runner._session_data[Key('userB', 'session')] == {
        'start_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        'end_time': datetime(2018, 3, 7, 22, 35, 31, tzinfo=tzutc()),
        'events': 1,
        'country': '',
        'continent': ''
    }

    # Window DTC output
    assert local_runner._window_data['userA'] == [{
        'last_session.events': 1,
        'last_day.total_events': 1
    }]
    assert local_runner._window_data['userB'] == []

    # Variables should not be stored
    assert Key('userA', 'vars') not in local_runner._session_data
