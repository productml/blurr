from time import sleep
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

        self.dynamodb_client = boto3.client('dynamodb')
        self.dynamodb_resource = boto3.resource('dynamodb')

        # If table does not exist, create the table
        if self.table_name not in self.dynamodb_client.list_tables()['TableNames']:
            self.dynamodb_client.create_table(
                AttributeDefinitions=[
                    {
                        'AttributeName': 'identity',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'dimensions',
                        'AttributeType': 'S'
                    }
                ],
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'identity',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'dimensions',
                        'KeyType': 'RANGE'
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': self.rcu,
                    'WriteCapacityUnits': self.wcu
                }
            )

        while self.table_name not in self.dynamodb_client.list_tables()['TableNames'] or \
                self.dynamodb_client.describe_table(TableName=self.table_name).get('Table', {}).get('TableStatus',
                                                                                                    '') != 'ACTIVE':
            sleep(1)

        self.table = self.dynamodb_resource.Table(self.table_name)

    @staticmethod
    def dimensions(key: Key):
        return key.group + key.PARTITION + key.timestamp.isoformat() if key.timestamp else key.group

    @staticmethod
    def clean(item: Dict[str, Any]) -> Dict[str, Any]:
        item.pop('identity', None)
        item.pop('dimensions', None)
        return item

    def get(self, key: Key) -> Any:
        item = self.table.get_item(Key={
            'identity': key.identity,
            'dimensions': self.dimensions(key)
        }).get('Item', None)

        if not item:
            return None

        return self.clean(item)

    def get_range(self, start: Key, end: Key = None, count: int = 0) -> List[Tuple[Key, Any]]:

        if end and count:
            raise ValueError('Only one of `end` or `count` can be set')

        dimension_key_condition = DynamoKey('dimensions')

        if end:
            dimension_key_condition = dimension_key_condition.between(self.dimensions(start), self.dimensions(end))
        else:
            dimension_key_condition = dimension_key_condition.gt(self.dimensions(start)) if count > 0 \
                else dimension_key_condition.lt(self.dimensions(start))

        response = self.table.query(
            Limit=abs(count) if count else 1000,
            KeyConditionExpression=DynamoKey('identity').eq(start.identity) & dimension_key_condition,
            ScanIndexForward=count >= 0,
        )

        def prepare_record(record: Dict[str, Any]) -> Tuple[Key, Any]:
            dimensions = record['dimensions'].split(Key.PARTITION)
            key = Key(record['identity'], dimensions[0], parser.parse(dimensions[1]))
            return key, self.clean(record)

        records = [prepare_record(item) for item in response['Items']] if \
            'Items' in response else []

        # Ignore the starting record
        if records[0][0] == start or records[0][0] == end:
            del records[0]

        if records[-1][0] == start or records[-1][0] == end:
            del records[-1]

        return records

    def save(self, key: Key, item: Any) -> None:
        item['identity'] = key.identity
        item['dimensions'] = self.dimensions(key)
        self.table.put_item(Item=item)

    def delete(self, key: Key) -> None:
        pass

    def finalize(self) -> None:
        pass
