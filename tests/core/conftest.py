from datetime import datetime, timezone

from pytest import fixture

from blurr.core.type import Type
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.store.memory_store import MemoryStore


@fixture
def stream_dtc_name() -> str:
    return "test_stream_dtc"


@fixture
def mem_store_name() -> str:
    return 'memstore'


@fixture
def schema_loader_with_mem_store(stream_dtc_name: str) -> SchemaLoader:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({
        'Name': 'memstore',
        'Type': Type.BLURR_STORE_MEMORY
    }, stream_dtc_name)
    store = schema_loader.get_store(stream_dtc_name + '.' + name)
    init_memory_store(store)
    return schema_loader


def init_memory_store(store: MemoryStore) -> None:
    store.save(Key('user1', 'state'), {'variable_1': 1, 'variable_a': 'a', 'variable_true': True})

    date = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    store.save(Key('user1', 'session', date), {'events': 1, '_start_time': date.isoformat()})

    date = datetime(2018, 3, 7, 20, 35, 35, 0, timezone.utc)
    store.save(Key('user1', 'session', date), {'events': 2, '_start_time': date.isoformat()})

    date = datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc)
    store.save(Key('user1', 'session', date), {'events': 3, '_start_time': date.isoformat()})

    date = datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc)
    store.save(Key('user1', 'session', date), {'events': 4, '_start_time': date.isoformat()})

    date = datetime(2018, 3, 7, 23, 40, 31, 0, timezone.utc)
    store.save(Key('user1', 'session', date), {'events': 5, '_start_time': date.isoformat()})

    date = datetime(2018, 3, 8, 23, 40, 31, 0, timezone.utc)
    store.save(Key('user1', 'session'), {'events': 6, '_start_time': date.isoformat()})
