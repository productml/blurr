from blurr.core.store import Key
from blurr.runner.local_runner import LocalRunner
from datetime import datetime, timezone


def test_local_runner():
    local_runner = LocalRunner(['tests/runner/data/raw.json'], '',
                               'tests/runner/data/sample.yml')
    local_runner.execute()

    assert local_runner._user_transformer['userA'].snapshot['session'][
        'events'] == 2
    assert local_runner._user_transformer['userB'].snapshot['session'][
        'events'] == 1
    assert local_runner._user_transformer['userC'].snapshot['session'][
        'events'] == 1

    assert local_runner._user_transformer['userA'].schema.stores['memory'].get(
        Key('userA', 'session'))['events'] == 2
    assert local_runner._user_transformer['userA'].schema.stores['memory'].get(
        Key('userA', 'session',
            datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc)))['events'] == 1

    assert local_runner._user_transformer['userA'].schema.stores['memory'].get(
        Key('userA', 'session',
            datetime(2018, 3, 7, 22, 35, 31, 0,
                     timezone.utc)))['country'] == 'US'
    assert local_runner._user_transformer['userA'].schema.stores['memory'].get(
        Key('userA', 'session',
            datetime(2018, 3, 7, 22, 35, 31, 0,
                     timezone.utc)))['continent'] == 'North America'

    assert local_runner._user_transformer['userA'].schema.stores['memory'].get(
        Key('userA', 'state'))['country'] == 'IN'
    assert local_runner._user_transformer['userA'].schema.stores['memory'].get(
        Key('userA', 'state'))['continent'] == 'World'

    # Variables should not be stored
    assert not local_runner._user_transformer['userA'].schema.stores['memory'].get(
        Key('userA', 'vars'))
