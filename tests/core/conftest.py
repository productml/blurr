from datetime import datetime, timezone
from typing import Dict, Any

from pytest import fixture

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType
from blurr.core.type import Type
from blurr.store.memory_store import MemoryStore
from blurr.core.aggregate import AggregateSchema


@fixture
def stream_bts_name() -> str:
    return "test_stream_bts"


@fixture
def mem_store_name() -> str:
    return 'memstore'


@fixture
def schema_loader_with_mem_store(stream_bts_name: str) -> SchemaLoader:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec({
        'Name': 'memstore',
        'Type': Type.BLURR_STORE_MEMORY
    }, stream_bts_name)
    store = schema_loader.get_store(stream_bts_name + '.' + name)
    init_memory_store(store)
    return schema_loader


def init_memory_store(store: MemoryStore) -> None:
    store.save(
        Key(KeyType.DIMENSION, 'user1', 'state'), {
            'variable_1': 1,
            'variable_a': 'a',
            'variable_true': True
        })

    date = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    store.save(
        Key(KeyType.TIMESTAMP, 'user1', 'session', [], date), {
            'events': 1,
            '_start_time': date.isoformat()
        })
    store.save(
        Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA', 'session1']), {
            'events': 1,
            '_start_time': date.isoformat()
        })

    date = datetime(2018, 3, 7, 20, 35, 35, 0, timezone.utc)
    store.save(
        Key(KeyType.TIMESTAMP, 'user1', 'session', [], date), {
            'events': 2,
            '_start_time': date.isoformat()
        })
    store.save(
        Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimB', 'session2']), {
            'events': 2,
            '_start_time': date.isoformat()
        })

    date = datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc)
    store.save(
        Key(KeyType.TIMESTAMP, 'user1', 'session', [], date), {
            'events': 3,
            '_start_time': date.isoformat()
        })
    store.save(
        Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA', 'session3']), {
            'events': 3,
            '_start_time': date.isoformat()
        })

    date = datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc)
    store.save(
        Key(KeyType.TIMESTAMP, 'user1', 'session', [], date), {
            'events': 4,
            '_start_time': date.isoformat()
        })
    store.save(
        Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimB', 'session4']), {
            'events': 4,
            '_start_time': date.isoformat()
        })

    date = datetime(2018, 3, 7, 23, 40, 31, 0, timezone.utc)
    store.save(
        Key(KeyType.TIMESTAMP, 'user1', 'session', [], date), {
            'events': 5,
            '_start_time': date.isoformat()
        })
    store.save(
        Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA', 'session5']), {
            'events': 5,
            '_start_time': date.isoformat()
        })

    date = datetime(2018, 3, 8, 23, 40, 31, 0, timezone.utc)
    store.save(
        Key(KeyType.TIMESTAMP, 'user1', 'session', [], date), {
            'events': 6,
            '_start_time': date.isoformat()
        })
    store.save(
        Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimB', 'session6']), {
            'events': 6,
            '_start_time': date.isoformat()
        })


def assert_aggregate_snapshot_equals(left: Dict[str, Any], right: Dict[str, Any], verify_identity = False):
    """ Compares two aggregate snapshots for equality after removing the implicitly added fields """

    left.pop(AggregateSchema.ATTRIBUTE_FIELD_PROCESSED_TRACKER, None)
    right.pop(AggregateSchema.ATTRIBUTE_FIELD_PROCESSED_TRACKER, None)

    if not verify_identity:
        left.pop(AggregateSchema.ATTRIBUTE_FIELD_IDENTITY, None)
        right.pop(AggregateSchema.ATTRIBUTE_FIELD_IDENTITY, None)

    assert len(left) == len(right)
    assert left == right
