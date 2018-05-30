from datetime import datetime
from typing import Any, Dict, List, Tuple

import boto3
from boto3.dynamodb.conditions import Key as DynamoKey, Attr
from dateutil import parser

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Store, Key, StoreSchema
from blurr.core.store_key import KeyType


class DynamoStoreSchema(StoreSchema):
    ATTRIBUTE_TABLE = 'Table'
    ATTRIBUTE_READ_CAPACITY_UNITS = 'ReadCapacityUnits'
    ATTRIBUTE_WRITE_CAPACITY_UNITS = 'WriteCapacityUnits'
    QUERY_LIMIT = 1000

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)
        self.table_name = self._spec.get(self.ATTRIBUTE_TABLE, None)
        self.rcu = self._spec.get(self.ATTRIBUTE_READ_CAPACITY_UNITS, 5)
        self.wcu = self._spec.get(self.ATTRIBUTE_WRITE_CAPACITY_UNITS, 5)
        self.query_limit = self.QUERY_LIMIT

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_TABLE)


class DynamoStore(Store):
    """
    Dynamo store implementation
    """

    def __init__(self, schema: DynamoStoreSchema) -> None:
        self._schema = schema
        self._dynamodb_resource = DynamoStore.get_dynamodb_resource()
        self._table = self._dynamodb_resource.Table(self._schema.table_name)

        # Test that the table exists.  Create a new one otherwise
        try:
            self._table.creation_date_time
        except self._dynamodb_resource.meta.client.exceptions.ResourceNotFoundException:
            self._table = self._dynamodb_resource.create_table(
                TableName=self._schema.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'partition_key',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'range_key',
                        'KeyType': 'RANGE'
                    },
                ],
                AttributeDefinitions=[{
                    'AttributeName': 'partition_key',
                    'AttributeType': 'S'
                }, {
                    'AttributeName': 'range_key',
                    'AttributeType': 'S'
                }],
                ProvisionedThroughput={
                    'ReadCapacityUnits': self._schema.rcu,
                    'WriteCapacityUnits': self._schema.wcu
                })
            # Wait until the table creation is complete
            self._table.meta.client.get_waiter('table_exists').wait(
                TableName=self._schema.table_name, WaiterConfig={'Delay': 5})

    @staticmethod
    # This is separate out as a separate function so that this can be mocked in unit tests.
    def get_dynamodb_resource() -> Any:
        return boto3.resource('dynamodb')

    @staticmethod
    def clean_for_get(item: Dict[str, Any]) -> Dict[str, Any]:
        item.pop('partition_key', None)
        item.pop('range_key', None)
        return item

    @staticmethod
    def clean_item_for_save(item: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in item.items() if v}

    def prepare_record(self, record: Dict[str, Any]) -> Tuple[Key, Any]:
        key = Key.parse_sort_key(record['partition_key'], record['range_key'])
        return key, self.clean_for_get(record)

    def get(self, key: Key) -> Any:
        item = self._table.get_item(Key={
            'partition_key': key.identity,
            'range_key': key.sort_key
        }).get('Item', None)

        if not item:
            return None

        return self.clean_for_get(item)

    def _get_range_timestamp_key(self, start: Key, end: Key,
                                 count: int = 0) -> List[Tuple[Key, Any]]:
        sort_key_condition = DynamoKey('range_key').between(start.sort_key, end.sort_key)
        # Limit is set to count+1 because for items where the start key matches exactly
        # KeyConditionExpression passes and FilterExpression fails.
        response = self._table.query(
            Limit=abs(count) + 1 if count else self._schema.query_limit,
            KeyConditionExpression=DynamoKey('partition_key').eq(
                start.identity) & sort_key_condition,
            FilterExpression=Attr('_start_time').gt(start.timestamp.isoformat()) &
            Attr('_start_time').lt(end.timestamp.isoformat()),
            ScanIndexForward=count >= 0,
        )
        items = sorted([self.prepare_record(item) for item in response.get('Items', [])])
        if count:
            items = self._restrict_items_to_count(items, count)
        return items

    def _get_range_dimension_key(self,
                                 base_key: Key,
                                 start_time: datetime,
                                 end_time: datetime = None,
                                 count: int = 0) -> List[Tuple[Key, Any]]:
        # A smaller limit cannot be set when abs(count) > 0 because all items need to be
        # returned to find the count number of elements in a sorted manner.
        # TODO: Improve count query performance by using a secondary index.
        response = self._table.query(
            Limit=self._schema.query_limit,
            KeyConditionExpression=DynamoKey('partition_key').eq(
                base_key.identity) & DynamoKey('range_key').begins_with(base_key.sort_prefix_key),
            FilterExpression=Attr('_start_time').gt(start_time.isoformat()) &
            Attr('_start_time').lt(end_time.isoformat()),
            ScanIndexForward=count >= 0,
        )
        items = sorted(
            [self.prepare_record(item) for item in response.get('Items', [])],
            key=lambda i: i[1].get('_start_time', datetime.min.isoformat()))
        if count:
            items = self._restrict_items_to_count(items, count)
        return items

    def get_all(self, identity: str) -> Dict[Key, Any]:
        response = self._table.query(KeyConditionExpression=DynamoKey('partition_key').eq(identity))
        return dict([self.prepare_record(item)
                     for item in response['Items']] if 'Items' in response else [])

    def save(self, key: Key, item: Any) -> None:
        item['partition_key'] = key.identity
        item['range_key'] = key.sort_key
        self._table.put_item(Item=self.clean_item_for_save(item))

    def delete(self, key: Key) -> None:
        pass

    def finalize(self) -> None:
        pass
