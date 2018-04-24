from typing import Any, Dict, List, Tuple

import boto3
from boto3.dynamodb.conditions import Key as DynamoKey
from dateutil import parser

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Store, Key

ATTRIBUTE_TABLE = 'Table'
ATTRIBUTE_READ_CAPACITY_UNITS = 'ReadCapacityUnits'
ATTRIBUTE_WRITE_CAPACITY_UNITS = 'WriteCapacityUnits'


class DynamoStore(Store):
    """
    In-memory store implementation
    """

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)
        spec = schema_loader.get_schema_spec(fully_qualified_name)
        self.table_name = spec[ATTRIBUTE_TABLE]
        self.rcu = spec.get(ATTRIBUTE_READ_CAPACITY_UNITS, 5)
        self.wcu = spec.get(ATTRIBUTE_WRITE_CAPACITY_UNITS, 5)

        self.dynamodb_resource = boto3.resource('dynamodb')

        self.table = self.dynamodb_resource.Table(self.table_name)

        # Test that the table exists.  Create a new one otherwise
        try:
            self.table.creation_date_time
        except self.dynamodb_resource.meta.client.exceptions.ResourceNotFoundException:
            self.table = self.dynamodb_resource.create_table(
                TableName=self.table_name,
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
                    'ReadCapacityUnits': self.rcu,
                    'WriteCapacityUnits': self.wcu
                })
            # Wait until the table creation is complete
            self.table.meta.client.get_waiter('table_exists').wait(
                TableName=self.table_name, WaiterConfig={'Delay': 5})

    @staticmethod
    def dimensions(key: Key):
        return key.group + (key.PARTITION + key.timestamp.isoformat() if key.timestamp else '')

    @staticmethod
    def clean(item: Dict[str, Any]) -> Dict[str, Any]:
        item.pop('partition_key', None)
        item.pop('range_key', None)
        return item

    def get(self, key: Key) -> Any:
        item = self.table.get_item(Key={
            'partition_key': key.identity,
            'range_key': self.dimensions(key)
        }).get('Item', None)

        if not item:
            return None

        return self.clean(item)

    def get_range(self, start: Key, end: Key = None, count: int = 0) -> List[Tuple[Key, Any]]:

        if end and count:
            raise ValueError('Only one of `end` or `count` can be set')

        dimension_key_condition = DynamoKey('range_key')

        if end:
            dimension_key_condition = dimension_key_condition.between(
                self.dimensions(start), self.dimensions(end))
        else:
            dimension_key_condition = dimension_key_condition.gt(self.dimensions(start)) if count > 0 \
                else dimension_key_condition.lt(self.dimensions(start))

        response = self.table.query(
            Limit=abs(count) if count else 1000,
            KeyConditionExpression=DynamoKey('partition_key').eq(start.identity) &
            dimension_key_condition,
            ScanIndexForward=count >= 0,
        )

        def prepare_record(record: Dict[str, Any]) -> Tuple[Key, Any]:
            dimensions = record['range_key'].split(Key.PARTITION)
            key = Key(record['partition_key'], dimensions[0], parser.parse(dimensions[1]))
            return key, self.clean(record)

        records = [prepare_record(item) for item in response['Items']] if \
            'Items' in response else []

        # Ignore the starting record because `between` includes the records that match the boundary condition
        if records[0][0] == start or records[0][0] == end:
            del records[0]

        if records[-1][0] == start or records[-1][0] == end:
            del records[-1]

        return records

    def save(self, key: Key, item: Any) -> None:
        item['partition_key'] = key.identity
        item['range_key'] = self.dimensions(key)
        self.table.put_item(Item=item)

    def delete(self, key: Key) -> None:
        pass

    def finalize(self) -> None:
        pass
