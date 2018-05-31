from datetime import datetime, timezone

from pytest import fixture, mark

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType
from blurr.core.type import Type
from blurr.store.memory_store import MemoryStore


@fixture
def memory_store(schema_loader_with_mem_store, stream_bts_name, mem_store_name) -> MemoryStore:
    return schema_loader_with_mem_store.get_store(stream_bts_name + '.' + mem_store_name)


@fixture
def empty_memory_store() -> MemoryStore:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec({'Name': 'memstore', 'Type': Type.BLURR_STORE_MEMORY})
    return schema_loader.get_store(name)


def test_get(memory_store: MemoryStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'state')
    assert memory_store.get(key) == {'variable_1': 1, 'variable_a': 'a', 'variable_true': True}

    date = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    key = Key(KeyType.TIMESTAMP, 'user1', 'session', [], date)
    assert memory_store.get(key) == {'events': 1, '_start_time': date.isoformat()}


def test_set_simple(empty_memory_store) -> None:
    """
    Tests that the setter stores an item in memory that can be retrieved by the same key
    :return:
    """
    store = empty_memory_store
    store.save(Key(KeyType.DIMENSION, 'test_user', 'test_group'), 1)

    assert store.get(Key(KeyType.DIMENSION, 'test_user', 'test_group')) == 1


def test_set_get_date(empty_memory_store) -> None:
    """
    Tests that the timestamp is used as part of the key object
    """
    store = empty_memory_store
    now = datetime.utcnow()
    key = Key(KeyType.TIMESTAMP, 'user1', 'session', [], now)
    store.save(key, 'test')

    assert store.get(Key(KeyType.TIMESTAMP, 'user1', 'session', [], now)) == 'test'


@mark.parametrize('key_type_and_group', [(KeyType.TIMESTAMP, 'session'),
                                         (KeyType.DIMENSION, 'session_dim')])
def test_get_range_start_end_time(memory_store: MemoryStore, key_type_and_group) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    key = Key(key_type_and_group[0], 'user1', key_type_and_group[1])
    blocks = memory_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc),
                                    datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0,
                                                   timezone.utc).isoformat()


@mark.parametrize('key_type_and_group', [(KeyType.TIMESTAMP, 'session'),
                                         (KeyType.DIMENSION, 'session_dim')])
def test_get_range_count_positive(memory_store: MemoryStore, key_type_and_group) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    key = Key(key_type_and_group[0], 'user1', key_type_and_group[1])
    blocks = memory_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc), None, 2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0,
                                                   timezone.utc).isoformat()


@mark.parametrize('key_type_and_group', [(KeyType.TIMESTAMP, 'session'),
                                         (KeyType.DIMENSION, 'session_dim')])
def test_get_range_count_negative(memory_store: MemoryStore, key_type_and_group) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    key = Key(key_type_and_group[0], 'user1', key_type_and_group[1])
    blocks = memory_store.get_range(key, datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc), None,
                                    -2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0,
                                                   timezone.utc).isoformat()


@mark.parametrize('key_type_and_group', [(KeyType.TIMESTAMP, 'session'),
                                         (KeyType.DIMENSION, 'session_dim')])
def test_get_range_count_negative_from_first_element(memory_store: MemoryStore,
                                                     key_type_and_group) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    key = Key(key_type_and_group[0], 'user1', key_type_and_group[1])
    blocks = memory_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc), None,
                                    -2)
    assert len(blocks) == 0


def test_get_range_start_end_time_partial_dimensions_match(memory_store: MemoryStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA'])
    blocks = memory_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc),
                                    datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    assert len(blocks) == 1
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 21, 36, 31, 0,
                                                   timezone.utc).isoformat()


def test_get_range_count_positive_partial_dimensions_match(memory_store: MemoryStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA'])
    blocks = memory_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc), None, 2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 21, 36, 31, 0,
                                                   timezone.utc).isoformat()


def test_get_range_count_negative_partial_dimensions_match(memory_store: MemoryStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA'])
    blocks = memory_store.get_range(key, datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc), None,
                                    -2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 19, 35, 31, 0,
                                                   timezone.utc).isoformat()


def test_get_range_start_end_time_no_dimensions_match(memory_store: MemoryStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimC'])
    blocks = memory_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc),
                                    datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    assert len(blocks) == 0


def test_get_all(memory_store: MemoryStore) -> None:
    assert len(memory_store.get_all('user1')) == 13
