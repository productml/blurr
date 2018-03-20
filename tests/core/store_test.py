from pytest import fixture

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Key
from blurr.store.memory_store import MemoryStore
from datetime import datetime, timezone


@fixture
def memory_store() -> MemoryStore:
    schema_loader = SchemaLoader()
    schema_loader.add_schema({
        'Name': 'memstore',
        'Type': 'ProductML:DTC:Store:MemoryStore'
    })
    store = MemoryStore('memstore', schema_loader)
    store.save(
        Key('user1', 'state'), {
            'variable_1': 1,
            'variable_a': 'a',
            'variable_true': True
        })

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc)), {
                'events': 1,
                'start_time': datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc)
            }),

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 35, 35, 0, timezone.utc)), {
                'events': 2,
                'start_time': datetime(2018, 3, 7, 22, 35, 35, 0, timezone.utc)
            }),

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc)), {
                'events': 3,
                'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc)
            }),

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc)), {
                'events': 4,
                'start_time': datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc)
            }),

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 40, 31, 0, timezone.utc)), {
                'events': 5,
                'start_time': datetime(2018, 3, 7, 22, 40, 31, 0, timezone.utc)
            }),

    return store


@fixture
def empty_memory_store() -> MemoryStore:
    schema_loader = SchemaLoader()
    schema_loader.add_schema({
        'Name': 'memstore',
        'Type': 'ProductML:DTC:Store:MemoryStore'
    })
    return MemoryStore('memstore', schema_loader)


def test_get(memory_store: MemoryStore) -> None:
    key = Key('user1', 'state')
    assert memory_store.get(key) == {
        'variable_1': 1,
        'variable_a': 'a',
        'variable_true': True
    }

    key = Key('user1', 'session',
              datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc))
    assert memory_store.get(key) == {
        'events': 1,
        'start_time': datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc)
    }


def test_set_simple(empty_memory_store) -> None:
    """
    Tests that the setter stores an item in memory that can be retrieved by the same key
    :return:
    """
    store = empty_memory_store
    store.save(Key('test_user', 'test_group'), 1)

    assert store.get(Key('test_user', 'test_group')) == 1


def test_set_get_date(empty_memory_store) -> None:
    """
    Tests that the timestamp is used as part of the key object
    """
    store = empty_memory_store
    now = datetime.utcnow()
    key = Key('user1', 'session', now)
    store.save(key, 'test')

    assert store.get(Key('user1', 'session', now)) == 'test'


def test_get_range_start_end(memory_store: MemoryStore) -> None:
    """
    Tests that the range get does not include the sessions that lie on the boundary
    """
    start = Key('user1', 'session',
                datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc))
    end = Key('user1', 'session',
              datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    sessions = memory_store.get_range(start, end)
    assert len(sessions) == 2
    assert sessions[0][1]['start_time'] == datetime(2018, 3, 7, 22, 35, 35, 0,
                                                    timezone.utc)


def test_get_range_start_count(memory_store: MemoryStore) -> None:
    """
    Tests that the range get does not include the sessions that lie on the boundary
    """
    start = Key('user1', 'session',
                datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc))
    sessions = memory_store.get_range(start, None, 2)
    assert len(sessions) == 2
    print(sessions)
    assert sessions[0][1]['start_time'] == datetime(2018, 3, 7, 22, 35, 35, 0,
                                                    timezone.utc)


def test_get_range_end_count(memory_store: MemoryStore) -> None:
    """
    Tests that the range get does not include the sessions that lie on the boundary
    """
    end = Key('user1', 'session',
              datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    sessions = memory_store.get_range(end, None, -2)
    assert len(sessions) == 2
    assert sessions[0][1]['start_time'] == datetime(2018, 3, 7, 22, 35, 35, 0,
                                                    timezone.utc)
