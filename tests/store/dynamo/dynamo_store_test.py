import time
from datetime import datetime, timezone
from typing import Dict, Any
from unittest import mock

import boto3
from pytest import fixture, mark

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType
from blurr.store.dynamo_store import DynamoStore
from tests.core.conftest import init_memory_store
from tests.store.dynamo.utils import DYNAMODB_KWARGS


@fixture
def dynamo_store_spec() -> Dict[str, Any]:
    return {
        'Name': 'dynamostore',
        'Type': 'Blurr:Store:Dynamo',
        'Table': '_unit_test' + '_' + str(int(time.time()))
    }


def override_boto3_dynamodb_resource(db_kwargs=DYNAMODB_KWARGS) -> Any:
    return boto3.resource('dynamodb', **db_kwargs)
    #return boto3.resource('dynamodb')


def get_boto3_dynamodb_client(db_kwargs=DYNAMODB_KWARGS) -> Any:
    return boto3.client('dynamodb', **db_kwargs)


@fixture
def store(dynamo_store_spec: Dict[str, Any]) -> DynamoStore:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(dynamo_store_spec)
    with mock.patch(
            'blurr.store.dynamo_store.DynamoStore.get_dynamodb_resource',
            new=override_boto3_dynamodb_resource):
        dynamo_store = schema_loader.get_store(name)
    yield dynamo_store
    dynamo_store._table.delete()


@fixture(scope='session')
def loaded_store() -> DynamoStore:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec({
        'Name': 'dynamostore',
        'Type': 'Blurr:Store:Dynamo',
        'Table': '_unit_test_range' + '_' + str(int(time.time()))
    })
    with mock.patch(
            'blurr.store.dynamo_store.DynamoStore.get_dynamodb_resource',
            new=override_boto3_dynamodb_resource):
        dynamo_store = schema_loader.get_store(name)

    init_memory_store(dynamo_store)
    yield dynamo_store
    dynamo_store._table.delete()


def test_schema_init(dynamo_store_spec: Dict[str, Any]) -> None:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(dynamo_store_spec)
    store_schema = schema_loader.get_schema_object(name)
    assert store_schema.name == dynamo_store_spec['Name']
    assert store_schema.table_name == dynamo_store_spec['Table']
    assert store_schema.rcu == 5
    assert store_schema.wcu == 5


def test_schema_init_with_read_write_units(dynamo_store_spec: Dict[str, Any]) -> None:
    dynamo_store_spec['ReadCapacityUnits'] = 10
    dynamo_store_spec['WriteCapacityUnits'] = 10
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(dynamo_store_spec)
    store_schema = schema_loader.get_schema_object(name)
    assert store_schema.rcu == 10
    assert store_schema.wcu == 10


@mock.patch(
    'blurr.store.dynamo_store.DynamoStore.get_dynamodb_resource',
    new=override_boto3_dynamodb_resource)
def test_table_creation_if_not_exist(dynamo_store_spec: Dict[str, Any]) -> None:
    table_name = '_unit_test_table_creation' + '_' + str(int(time.time()))
    dynamo_store_spec['Table'] = table_name
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(dynamo_store_spec)

    dynamodb_client = get_boto3_dynamodb_client()
    # Check table does not exist
    assert table_name not in dynamodb_client.list_tables()['TableNames']

    # Test that the store is created if it does not exist
    dynamo_store = schema_loader.get_store(name)
    assert table_name in dynamodb_client.list_tables()['TableNames']
    assert dynamo_store._table.item_count == 0

    dynamo_store._table.put_item(Item={'partition_key': 'part', 'range_key': 'range'})
    item = dynamo_store._table.get_item(
        Key={
            'partition_key': 'part',
            'range_key': 'range'
        }, ConsistentRead=True).get('Item', None)

    assert item

    # Ensure the same store is reused the next time
    dynamo_store2 = schema_loader.get_store(name)

    item = dynamo_store2._table.get_item(
        Key={
            'partition_key': 'part',
            'range_key': 'range'
        }, ConsistentRead=True).get('Item', None)

    assert item

    dynamo_store._table.delete()


def test_save_simple(store: DynamoStore) -> None:
    store.save(
        Key(KeyType.DIMENSION, 'test_user', 'test_group'), {
            'string_field': 'string',
            'int_field': 1
        })
    assert store.get(Key(KeyType.DIMENSION, 'test_user', 'test_group')) == {
        'string_field': 'string',
        'int_field': 1
    }


def test_save_time(store: DynamoStore) -> None:
    start_time = datetime(2018, 1, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
    store.save(
        Key(KeyType.TIMESTAMP, 'test_user', 'test_group', [], start_time), {
            'string_field': 'string2',
            'int_field': 2
        })
    assert store.get(Key(KeyType.TIMESTAMP, 'test_user', 'test_group', [], start_time)) == {
        'string_field': 'string2',
        'int_field': 2
    }


@mark.parametrize('key_type_and_group', [(KeyType.TIMESTAMP, 'session'),
                                         (KeyType.DIMENSION, 'session_dim')])
def test_get_range_start_end_time(loaded_store: DynamoStore, key_type_and_group) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    key = Key(key_type_and_group[0], 'user1', key_type_and_group[1])
    blocks = loaded_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc),
                                    datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0,
                                                   timezone.utc).isoformat()


@mark.parametrize('key_type_and_group', [(KeyType.TIMESTAMP, 'session'),
                                         (KeyType.DIMENSION, 'session_dim')])
def test_get_range_count_positive(loaded_store: DynamoStore, key_type_and_group) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    key = Key(key_type_and_group[0], 'user1', key_type_and_group[1])
    blocks = loaded_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc), None, 2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0,
                                                   timezone.utc).isoformat()


@mark.parametrize('key_type_and_group', [(KeyType.TIMESTAMP, 'session'),
                                         (KeyType.DIMENSION, 'session_dim')])
def test_get_range_count_negative(loaded_store: DynamoStore, key_type_and_group) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    key = Key(key_type_and_group[0], 'user1', key_type_and_group[1])
    blocks = loaded_store.get_range(key, datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc), None,
                                    -2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 20, 35, 35, 0,
                                                   timezone.utc).isoformat()


@mark.parametrize('key_type_and_group', [(KeyType.TIMESTAMP, 'session'),
                                         (KeyType.DIMENSION, 'session_dim')])
def test_get_range_count_negative_from_first_element(loaded_store: DynamoStore,
                                                     key_type_and_group) -> None:
    """
    Tests that the range get does not include the blocks that lie on the boundary
    """
    key = Key(key_type_and_group[0], 'user1', key_type_and_group[1])
    blocks = loaded_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc), None,
                                    -2)
    assert len(blocks) == 0


def test_get_range_start_end_time_partial_dimensions_match(loaded_store: DynamoStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA'])
    blocks = loaded_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc),
                                    datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    assert len(blocks) == 1
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 21, 36, 31, 0,
                                                   timezone.utc).isoformat()


def test_get_range_count_positive_partial_dimensions_match(loaded_store: DynamoStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA'])
    blocks = loaded_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc), None, 2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 21, 36, 31, 0,
                                                   timezone.utc).isoformat()


def test_get_range_count_negative_partial_dimensions_match(loaded_store: DynamoStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimA'])
    blocks = loaded_store.get_range(key, datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc), None,
                                    -2)
    assert len(blocks) == 2
    assert blocks[0][1]['_start_time'] == datetime(2018, 3, 7, 19, 35, 31, 0,
                                                   timezone.utc).isoformat()


def test_get_range_start_end_time_no_dimensions_match(loaded_store: DynamoStore) -> None:
    key = Key(KeyType.DIMENSION, 'user1', 'session_dim', ['dimC'])
    blocks = loaded_store.get_range(key, datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc),
                                    datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc))
    assert len(blocks) == 0


def test_get_all(loaded_store: DynamoStore) -> None:
    assert len(loaded_store.get_all('user1')) == 13
