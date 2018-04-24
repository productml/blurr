import time
from datetime import datetime, timezone
from typing import Dict, Any

from pytest import fixture

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.store.dynamo_store import DynamoStore

import boto3

@fixture
def dynamo_store_spec() -> Dict[str, Any]:
    return {
        'Name': 'dynamostore',
        'Type': 'Blurr:Store:DynamoStore',
        'Table': '_unit_test' + '_' + str(int(time.time()))
    }


@fixture
def store(dynamo_store_spec) -> DynamoStore:
    schema_loader = SchemaLoader()
    schema_loader.add_schema(dynamo_store_spec)
    dynamo_store = DynamoStore('dynamostore', schema_loader)
    yield dynamo_store
    dynamo_store.table.delete()


def test_table_creation_if_not_exist(dynamo_store_spec):
    table_name = '_unit_test_table_creation' + '_' + str(int(time.time()))
    dynamo_store_spec['Table'] = table_name
    schema_loader = SchemaLoader()
    schema_loader.add_schema(dynamo_store_spec)

    dynamodb_client = boto3.client('dynamodb')
    # Check table does not exist
    assert table_name not in dynamodb_client.list_tables()['TableNames']

    # Test that the store is created if it does not exist
    dynamo_store = DynamoStore('dynamostore', schema_loader)
    assert table_name in dynamodb_client.list_tables()['TableNames']
    assert dynamo_store.table.item_count == 0

    dynamo_store.table.put_item(Item={'partition_key': 'part', 'range_key': 'range'})
    item = dynamo_store.table.get_item(Key={
        'partition_key': 'part',
        'range_key': 'range'
    }, ConsistentRead=True).get('Item', None)

    assert item

    # Ensure the same store is reused the next time
    dynamo_store2 = DynamoStore('dynamostore', schema_loader)

    item = dynamo_store2.table.get_item(Key={
        'partition_key': 'part',
        'range_key': 'range'
    }, ConsistentRead=True).get('Item', None)

    assert item

    dynamo_store.table.delete()


def test_save_simple(store: DynamoStore) -> None:
    store.save(Key('test_user', 'test_group'), {'string_field': 'string', 'int_field': 1})
    assert store.get(Key('test_user', 'test_group')) == {'string_field': 'string', 'int_field': 1}


def test_save_time(store: DynamoStore) -> None:
    start_time = datetime(2018, 1, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
    store.save(Key('test_user', 'test_group', start_time), {'string_field': 'string2', 'int_field': 2})
    assert store.get(Key('test_user', 'test_group', start_time)) == {'string_field': 'string2', 'int_field': 2}


def test_get_range(store: DynamoStore) -> None:
    store.save(Key('test_user', 'test_group', datetime(2018, 1, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string', 'int_field': 1})
    store.save(Key('test_user2', 'test_group', datetime(2018, 1, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string', 'int_field': 1})
    store.save(Key('test_user', 'test_group', datetime(2018, 1, 1, 2, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string2', 'int_field': 2})
    store.save(Key('test_user', 'test_group', datetime(2018, 1, 1, 3, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string3', 'int_field': 3})
    store.save(Key('test_user', 'test_group', datetime(2018, 1, 1, 4, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string4', 'int_field': 4})
    store.save(Key('test_user', 'test_group', datetime(2018, 1, 1, 5, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string5', 'int_field': 5})
    store.save(Key('test_user', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string6', 'int_field': 6})
    store.save(Key('test_user2', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string6', 'int_field': 6})
    store.save(Key('test_user', 'test_group', datetime(2018, 1, 1, 7, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string7', 'int_field': 7})
    store.save(Key('test_user', 'test_group', datetime(2018, 1, 1, 8, 1, 1, 1, tzinfo=timezone.utc)
                   ), {'string_field': 'string8', 'int_field': 8})

    items = store.get_range(Key('test_user', 'test_group', datetime(2018, 1, 1, 2, 1, 1, 1, tzinfo=timezone.utc)),
                            Key('test_user', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 1, tzinfo=timezone.utc)))

    assert len(items) == 3
    assert items[0][1]['int_field'] == 3
    assert items[-1][1]['int_field'] == 5

    items = store.get_range(Key('test_user', 'test_group', datetime(2018, 1, 1, 2, 1, 1, 0, tzinfo=timezone.utc)),
                            Key('test_user', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 2, tzinfo=timezone.utc)))

    assert len(items) == 5
    assert items[0][1]['int_field'] == 2
    assert items[-1][1]['int_field'] == 6

    items = store.get_range(Key('test_user', 'test_group', datetime(2018, 1, 1, 2, 1, 1, 1, tzinfo=timezone.utc)),
                            None, 3)

    assert len(items) == 3
    assert items[0][1]['int_field'] == 3
    assert items[-1][1]['int_field'] == 5

    items = store.get_range(Key('test_user', 'test_group', datetime(2018, 1, 1, 6, 1, 1, 1, tzinfo=timezone.utc)),
                            None, -3)

    assert len(items) == 3
    assert items[0][1]['int_field'] == 5
    assert items[-1][1]['int_field'] == 3
