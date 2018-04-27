from typing import Dict, Any

import pytest
from pytest import fixture, raises

from blurr.core.aggregate import AggregateSchema
from blurr.core.type import Type
from blurr.core.errors import InvalidSchemaError
from blurr.core.schema_loader import SchemaLoader
from blurr.core.validator import ATTRIBUTE_NAME


@fixture
def aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_BLOCK,
        'Name': 'user',
        'Fields': [{
            'Name': 'event_count',
            'Type': Type.INTEGER,
            'Value': 5
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Type': Type.BLURR_STORE_MEMORY, 'Name': 'memory'}


class MockAggregateSchema(AggregateSchema):
    pass


def test_aggregate_schema_contains_identity_field(aggregate_schema_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(aggregate_schema_spec)

    aggregate_schema = MockAggregateSchema(name, schema_loader)
    assert len(aggregate_schema.nested_schema) == 2
    assert '_identity' in aggregate_schema.nested_schema


def test_aggregate_schema_initialization_with_store(aggregate_schema_spec, store_spec):
    aggregate_schema_spec['Store'] = 'memory'
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(aggregate_schema_spec)
    with pytest.raises(InvalidSchemaError, match="user.memory not declared in schema"):
        MockAggregateSchema(name, schema_loader)

    schema_loader.add_schema_spec(store_spec, 'user')
    aggregate_schema = MockAggregateSchema(name, schema_loader)
    assert aggregate_schema.store is not None
    assert aggregate_schema.store.name == 'memory'


def test_aggregate_schema_initialization_without_store(aggregate_schema_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(aggregate_schema_spec)
    aggregate_schema = MockAggregateSchema(name, schema_loader)
    assert aggregate_schema.store is None


def test_aggregate_schema_missing_fields_attribute_raises_error(aggregate_schema_spec):
    del aggregate_schema_spec[AggregateSchema.ATTRIBUTE_FIELDS]

    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(aggregate_schema_spec)

    with raises(InvalidSchemaError,
                match='`{field}:` missing in section `{name}`'.format(
                    field=AggregateSchema.ATTRIBUTE_FIELDS,
                    name=aggregate_schema_spec[ATTRIBUTE_NAME])):
        MockAggregateSchema(name, schema_loader)
