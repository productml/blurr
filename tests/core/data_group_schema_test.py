import yaml
from typing import Dict, Any
from pytest import fixture
import pytest

from blurr.core.data_group import DataGroupSchema
from blurr.core.errors import InvalidSchemaError
from blurr.core.schema_loader import SchemaLoader
from blurr.store.memory_store import MemoryStore


def get_data_group_schema_spec() -> Dict[str, Any]:
    return {
        'Type': 'ProductML:DTC:DataGroup:SessionAggregate',
        'Name': 'user',
        'Store': 'memory',
        'Filter': 'source.event_id in ["app_launched", "user_updated"]',
        'Fields': [{
            'Name': 'event_count',
            'Type': 'integer',
            'Value': 5
        }]
    }


def get_store_spec() -> Dict[str, Any]:
    return {'Type': 'ProductML:DTC:Store:MemoryStore', 'Name': 'memory'}


class MockDataGroupSchema(DataGroupSchema):
    pass


def test_data_group_schema_initialization_with_store():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(get_data_group_schema_spec())
    with pytest.raises(
            InvalidSchemaError, match="Store memory not declared is schema"):
        mock_data_group_schema = MockDataGroupSchema(name, schema_loader)

    schema_loader.add_schema(get_store_spec(), 'user')
    mock_data_group_schema = MockDataGroupSchema(name, schema_loader)
    assert mock_data_group_schema.store is not None
    assert mock_data_group_schema.store.name == 'memory'


def test_data_group_schema_initialization_without_store():
    schema_loader = SchemaLoader()
    data_group_schema_spec = get_data_group_schema_spec()
    del data_group_schema_spec['Store']
    name = schema_loader.add_schema(data_group_schema_spec)
    mock_data_group_schema = MockDataGroupSchema(name, schema_loader)
    assert mock_data_group_schema.store is None
