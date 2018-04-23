from datetime import datetime, timezone
from typing import Dict, Any

from pytest import fixture

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.store.dynamo_store import DynamoStore


@fixture(scope='session')
def dynamo_store_spec() -> Dict[str, Any]:
    return {
        'Name': 'dynamostore',
        'Type': 'Blurr:Store:DynamoStore',
        'Table': '_unit_test'
    }


@fixture
def store(dynamo_store_spec) -> DynamoStore:
    schema_loader = SchemaLoader()
    schema_loader.add_schema(dynamo_store_spec)
    return DynamoStore('dynamostore', schema_loader)


def test_save_simple(store: DynamoStore) -> None:
    store.save(Key('test_user', 'test_group'), {'string_field': 'string', 'int_field': 1})
    assert store.get(Key('test_user', 'test_group')) == {'string_field': 'string', 'int_field': 1}

def test_save_time(store: DynamoStore) -> None:
    start_time = datetime(2018,1,1,1,1,1,1, tzinfo=timezone.utc)
    store.save(Key('test_user', 'test_group', start_time), {'string_field': 'string2', 'int_field': 2})
    assert store.get(Key('test_user', 'test_group', start_time)) == {'string_field': 'string2', 'int_field': 2}

