from typing import Dict, Any
from blurr.store.memory_store import MemoryStore
from blurr.core.store import Key
from pytest import fixture
from datetime import datetime


@fixture()
def state() -> Dict[str, Any]:
    return {
        'user1-state': {
            'variable_1': 1,
            'variable_a': 'a',
            'variable_true': True
        },
        'user1-session-1521146000': {
            'events': 1
        },
        'user1-session-1521147000': {
            'events': 2
        },
        'user1-session-1521148000': {
            'events': 3
        },
        'user1-session-1521149000': {
            'events': 4
        },
        'user1-session-1521149662': {
            'events': 5
        }
    }


def test_get(state: Dict[str, Any]) -> None:
    store = MemoryStore(state)
    key = Key('user1', 'state')
    assert store.get(key) == state['user1-state']

    key = Key('user1', 'session', datetime(2018, 3, 15, 13, 33, 20))
    assert store.get(key) == state['user1-session-1521146000']


def test_set_simple() -> None:
    """
    Tests that the setter stores an item in memory that can be retrieved by the same key
    :return:
    """
    store = MemoryStore()
    store.save(Key('test_user', 'test_group'), 1)

    assert store.get(Key('test_user', 'test_group')) == 1


def test_set_get_date() -> None:
    """
    Tests that the timestamp is used as part of the key object
    """
    store = MemoryStore()
    now = datetime.utcnow()
    key = Key('user1', 'session', now)
    store.save(key, 'test')

    assert store.get(Key('user1', 'session', now)) == 'test'


def test_get_range(state: Dict[str, Any]) -> None:
    """
    Tests that the range get does not include the sessions that lie on the boundary
    """
    store = MemoryStore(state)
    start = Key('user1', 'session', datetime(2018, 3, 15, 13, 33, 20))
    end = Key('user1', 'session', datetime(2018, 3, 15, 14, 23, 20))
    sessions = store.get_range(start, end)
    assert len(sessions) == 2
    assert list(sessions)[0].timestamp == datetime(2018, 3, 15, 13, 50)


