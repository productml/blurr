from typing import Any, Dict, List, Tuple

from datetime import datetime, timezone

from boto3.dynamodb.conditions import Key as DynamoKey

from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Store, Key
import boto3


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

        self.table = self.dynamodb_resource.Table(self.table_name)

    def get(self, key: Key) -> Any:
        if not key.timestamp:
            key_dict = {
                    'identity': key.identity,
                    'dimensions': key.group
                }
            if key.timestamp:
                key_dict['start_time'] = key.timestamp.isoformat()

            item = self.table.get_item(Key=key_dict).get('Item', None)
        else:
            response = self.table.query(
                Limit=1,
                KeyConditionExpression=DynamoKey('identity').eq(key.identity) & DynamoKey('dimensions').eq(key.group),
                FilterExpression=DynamoKey('start_time').eq(key.timestamp.isoformat())
            )
            if 'Items' not in response or len(response['Items']) == 0:
                return None

            item = response['Items'][0]

        if not item:
            return None

        del item['identity']
        del item['dimensions']
        item.pop('start_time', None)

        return item


    def get_all(self) -> Dict[Key, Any]:
        return self._cache

    def get_range(self, start: Key, end: Key = None, count: int = 0) -> List[Tuple[Key, Any]]:
        if end and count:
            raise ValueError('Only one of `end` or `count` can be set')

        if end is not None and end < start:
            start, end = end, start

        if not count:
            # TODO: Come up with a better way of getting this range for instances where key
            # does not have a timestamp.
            items_in_range = []
            for key, item in self._cache.items():
                if start < key < end:
                    items_in_range.append((key, item))
                elif key.timestamp is None:
                    item_ts = item.get('_start_time', datetime.min)
                    item_ts = item_ts if item_ts.tzinfo else item_ts.replace(tzinfo=timezone.utc)
                    if start.timestamp < item_ts < end.timestamp:
                        items_in_range.append((key, item))
                        continue
            return items_in_range
        else:
            filter_condition = (lambda i: i[0] > start) if count > 0 else (lambda i: i[0] < start)

            items = sorted(filter(filter_condition, list(self._cache.items())))

            if abs(count) > len(items):
                count = DynamoStore._sign(count) * len(items)

            if count < 0:
                return items[count:]
            else:
                return items[:count]

    @staticmethod
    def _sign(x: int) -> int:
        return (1, -1)[x < 0]

    def save(self, key: Key, item: Any) -> None:
        item['identity'] = key.identity
        item['dimensions'] = key.group
        if key.timestamp:
            item['start_time'] = key.timestamp.isoformat()
        self.table.put_item(Item=item)

    def delete(self, key: Key) -> None:
        self._cache.pop(key, None)

    def finalize(self) -> None:
        pass
