import time
from datetime import datetime, timezone
from typing import Dict, Any

import boto3
from pytest import fixture

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.store.dynamo_store import DynamoStore


@fixture
def dynamo_store_spec() -> Dict[str, Any]:
    return {
        'Name': 'dynamostore',
        'Type': 'Blurr:Store:Dynamo',
        'Table': '_unit_test' + '_' + str(int(time.time()))
    }


@fixture
def store(dynamo_store_spec: Dict[str, Any]) -> DynamoStore:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(dynamo_store_spec)
    dynamo_store = schema_loader.get_store(name)
    yield dynamo_store
    dynamo_store._table.delete()


@fixture(scope='session')
def loaded_store() -> DynamoStore:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({
        'Name': 'dynamostore',
        'Type': 'Blurr:Store:Dynamo',
        'Table': '_unit_test_range' + '_' + str(int(time.time()))
    })
    dynamo_store = schema_loader.get_store(name)
    dynamo_store.save(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 1, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string',
            'int_field': 1
        })
    dynamo_store.save(
        Key('test_user2', 'test_group', datetime(2018, 1, 1, 1, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string',
            'int_field': 1
        })
    dynamo_store.save(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 2, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string2',
            'int_field': 2
        })
    dynamo_store.save(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 3, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string3',
            'int_field': 3
        })
    dynamo_store.save(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 4, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string4',
            'int_field': 4
        })
    dynamo_store.save(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 5, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string5',
            'int_field': 5
        })
    dynamo_store.save(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string6',
            'int_field': 6
        })
    dynamo_store.save(
        Key('test_user2', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string6',
            'int_field': 6
        })
    dynamo_store.save(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 7, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string7',
            'int_field': 7
        })
    dynamo_store.save(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 8, 1, 1, 1, tzinfo=timezone.utc)), {
            'string_field': 'string8',
            'int_field': 8
        })

    yield dynamo_store
    dynamo_store._table.delete()


def test_schema_init(dynamo_store_spec: Dict[str, Any]) -> None:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(dynamo_store_spec)
    store_schema = schema_loader.get_schema_object(name)
    assert store_schema.name == dynamo_store_spec['Name']
    assert store_schema.table_name == dynamo_store_spec['Table']
    assert store_schema.rcu == 5
    assert store_schema.wcu == 5


def test_schema_init_with_read_write_units(dynamo_store_spec: Dict[str, Any]) -> None:
    dynamo_store_spec['ReadCapacityUnits'] = 10
    dynamo_store_spec['WriteCapacityUnits'] = 10
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(dynamo_store_spec)
    store_schema = schema_loader.get_schema_object(name)
    assert store_schema.rcu == 10
    assert store_schema.wcu == 10


def test_table_creation_if_not_exist(dynamo_store_spec: Dict[str, Any]) -> None:
    table_name = '_unit_test_table_creation' + '_' + str(int(time.time()))
    dynamo_store_spec['Table'] = table_name
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(dynamo_store_spec)

    dynamodb_client = boto3.client('dynamodb')
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
    store.save(Key('test_user', 'test_group'), {'string_field': 'string', 'int_field': 1})
    assert store.get(Key('test_user', 'test_group')) == {'string_field': 'string', 'int_field': 1}


def test_save_time(store: DynamoStore) -> None:
    start_time = datetime(2018, 1, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
    store.save(
        Key('test_user', 'test_group', start_time), {
            'string_field': 'string2',
            'int_field': 2
        })
    assert store.get(Key('test_user', 'test_group', start_time)) == {
        'string_field': 'string2',
        'int_field': 2
    }


def test_get_range_items_on_boundary_are_removed(loaded_store: DynamoStore) -> None:
    items = loaded_store.get_range(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 2, 1, 1, 1, tzinfo=timezone.utc)),
        Key('test_user', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 1, tzinfo=timezone.utc)))

    assert len(items) == 3
    assert items[0][1]['int_field'] == 3
    assert items[-1][1]['int_field'] == 5


def test_get_range_no_items_on_boundary(loaded_store: DynamoStore) -> None:
    items = loaded_store.get_range(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 2, 1, 1, 0, tzinfo=timezone.utc)),
        Key('test_user', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 2, tzinfo=timezone.utc)))

    assert len(items) == 5
    assert items[0][1]['int_field'] == 2
    assert items[-1][1]['int_field'] == 6


def test_get_range_count_forward(loaded_store: DynamoStore) -> None:
    items = loaded_store.get_range(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 2, 1, 1, 1, tzinfo=timezone.utc)), None,
        3)

    assert len(items) == 3
    assert items[0][1]['int_field'] == 3
    assert items[-1][1]['int_field'] == 5


def test_get_range_count_backward(loaded_store: DynamoStore) -> None:
    items = loaded_store.get_range(
        Key('test_user', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 1, tzinfo=timezone.utc)), None,
        -3)

    assert len(items) == 3
    assert items[0][1]['int_field'] == 5
    assert items[-1][1]['int_field'] == 3


def test_get_all(loaded_store: DynamoStore) -> None:
    items = loaded_store.get_all('test_user')

    assert len(items) == 8
