from typing import Dict, Any

import pytest
from pytest import fixture

from blurr.core.aggregate import AggregateSchema
from blurr.core.type import Type
from blurr.core.errors import InvalidSchemaError
from blurr.core.schema_loader import SchemaLoader


@fixture
def aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_BLOCK,
        'Name': 'user',
        'Fields': [{
            'Name': 'event_count',
            'Type': Type.INTEGER,
            'Value': 5
        }, {
            'Name': 'missing_type',
            'Value': 'test'
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Type': Type.BLURR_STORE_MEMORY, 'Name': 'memory'}


class MockAggregateSchema(AggregateSchema):
    pass


def test_aggregate_schema_contains_identity_field(aggregate_schema_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(aggregate_schema_spec)

    aggregate_schema = MockAggregateSchema(name, schema_loader)
    assert len(aggregate_schema.nested_schema) == 3
    assert '_identity' in aggregate_schema.nested_schema


def test_aggregate_schema_initialization_with_store(aggregate_schema_spec, store_spec):
    aggregate_schema_spec['Store'] = 'memory'
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(aggregate_schema_spec)
    with pytest.raises(InvalidSchemaError, match="user.memory not declared in schema"):
        mock_agg = MockAggregateSchema(name, schema_loader)
        assert schema_loader.get_schema_object(mock_agg.store_fq_name)

    schema_loader.add_schema(store_spec, 'user')
    aggregate_schema = MockAggregateSchema(name, schema_loader)
    store = schema_loader.get_schema_object(mock_agg.store_fq_name)
    assert store is not None
    assert store.name == 'memory'
    assert aggregate_schema.store_name == 'memory'


def test_aggregate_schema_initialization_without_store(aggregate_schema_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(aggregate_schema_spec)
    aggregate_schema = MockAggregateSchema(name, schema_loader)
    assert aggregate_schema.store_name is None
    assert aggregate_schema.store_fq_name is None


def test_field_without_type_defaults_to_string(aggregate_schema_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(aggregate_schema_spec)
    aggregate_schema = MockAggregateSchema(name, schema_loader)
    missing_type_field = aggregate_schema.nested_schema['missing_type']

    assert Type.is_type_equal(missing_type_field.type, Type.STRING)
    assert missing_type_field.type_object is str
