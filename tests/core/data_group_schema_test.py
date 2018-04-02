from typing import Dict, Any

import pytest
from pytest import fixture

from blurr.core.data_group import DataGroupSchema
from blurr.core.errors import InvalidSchemaError
from blurr.core.schema_loader import SchemaLoader


@fixture
def data_group_schema_spec() -> Dict[str, Any]:
    return {
        'Type': 'Blurr:DataGroup:BlockAggregate',
        'Name': 'user',
        'Store': 'memory',
        'Fields': [{
            'Name': 'event_count',
            'Type': 'integer',
            'Value': 5
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Type': 'Blurr:Store:MemoryStore', 'Name': 'memory'}


class MockDataGroupSchema(DataGroupSchema):
    pass


def test_data_group_schema_initialization_with_store(data_group_schema_spec,
                                                     store_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(data_group_schema_spec)
    with pytest.raises(
            InvalidSchemaError, match="user.memory not declared in schema"):
        data_group_schema = MockDataGroupSchema(name, schema_loader)

    schema_loader.add_schema(store_spec, 'user')
    data_group_schema = MockDataGroupSchema(name, schema_loader)
    assert data_group_schema.store is not None
    assert data_group_schema.store.name == 'memory'


def test_data_group_schema_initialization_without_store(
        data_group_schema_spec):
    schema_loader = SchemaLoader()
    del data_group_schema_spec['Store']
    name = schema_loader.add_schema(data_group_schema_spec)
    data_group_schema = MockDataGroupSchema(name, schema_loader)
    assert data_group_schema.store is None
