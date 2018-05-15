from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate import AggregateSchema
from blurr.core.errors import RequiredAttributeError, SpecNotFoundError
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type


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

    MockAggregateSchema(name, schema_loader)
    assert isinstance(schema_loader.get_errors(name, True)[0], SpecNotFoundError)

    schema_loader.add_schema_spec(store_spec, 'user')
    aggregate_schema = MockAggregateSchema(name, schema_loader)
    store = schema_loader.get_store(aggregate_schema.store_schema.fully_qualified_name)
    assert store is not None
    assert store._schema.name == 'memory'
    assert aggregate_schema.store_schema.name == 'memory'


def test_aggregate_schema_initialization_without_store(aggregate_schema_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(aggregate_schema_spec)
    aggregate_schema = MockAggregateSchema(name, schema_loader)
    assert aggregate_schema.store_schema is None


def test_aggregate_schema_missing_attributes_adds_error(aggregate_schema_spec):
    del aggregate_schema_spec[AggregateSchema.ATTRIBUTE_FIELDS]

    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(aggregate_schema_spec)
    schema = MockAggregateSchema(name, schema_loader)

    assert 1 == len(schema.errors)
    assert isinstance(schema.errors[0], RequiredAttributeError)
    assert AggregateSchema.ATTRIBUTE_FIELDS == schema.errors[0].attribute
