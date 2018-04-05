from datetime import datetime, timezone

from pytest import fixture

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.store.memory_store import MemoryStore


@fixture
def memory_store(schema_loader_with_mem_store, stream_dtc_name, mem_store_name) -> MemoryStore:
    return schema_loader_with_mem_store.get_schema_object(stream_dtc_name + '.' + mem_store_name)


@fixture
def empty_memory_store() -> MemoryStore:
    schema_loader = SchemaLoader()
    schema_loader.add_schema({'Name': 'memstore', 'Type': 'Blurr:Store:MemoryStore'})
    return MemoryStore('memstore', schema_loader)


def test_get(memory_store: MemoryStore) -> None:
    key = Key('user1', 'state')
    assert memory_store.get(key) == {'variable_1': 1, 'variable_a': 'a', 'variable_true': True}

    date = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    key = Key('user1', 'session', date)
    assert memory_store.get(key) == {'events': 1, '_start_time': date}


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
    Tests that the range get does not include the blocks that lie on the boundary
    """
    start = Key('user1', 'session', datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc))
    end = Key('user1', 'session', datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    blocks = memory_store.get_range(start, end)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0, timezone.utc)


def test_get_range_start_count(memory_store: MemoryStore) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    start = Key('user1', 'session', datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc))
    blocks = memory_store.get_range(start, None, 2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0, timezone.utc)


def test_get_range_end_count(memory_store: MemoryStore) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    end = Key('user1', 'session', datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    blocks = memory_store.get_range(end, None, -2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0, timezone.utc)
