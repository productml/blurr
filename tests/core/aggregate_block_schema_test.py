from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate_block import BlockAggregateSchema
from blurr.core.errors import RequiredAttributeError
from blurr.core.evaluation import Expression
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type
from blurr.core.validator import ATTRIBUTE_INTERNAL


@fixture
def block_aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_BLOCK,
        'Name': 'user',
        'Filter': 'source.event_id in ["app_launched", "user_updated"]',
        'Store': 'memstore',
        'Split': True,
        'Fields': [{
            'Name': 'event_count',
            'Type': Type.INTEGER,
            'Value': 5
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Name': 'memstore', 'Type': Type.BLURR_STORE_MEMORY}

def match_fields(fields):
    expected_fields = [{
        'Name': '_identity',
        'Type': Type.STRING,
        'Value': 'identity',
        ATTRIBUTE_INTERNAL: True
    }, {
        'Name': '_start_time',
        'Type': Type.DATETIME,
        'Value': 'time if user._start_time is None else time if time < '
                 'user._start_time else user._start_time',
        ATTRIBUTE_INTERNAL: True
    }, {
        'Name': '_end_time',
        'Type': Type.DATETIME,
        'Value': 'time if user._end_time is None else time if time > '
                 'user._end_time else user._end_time',
        ATTRIBUTE_INTERNAL: True
    }, {
        'Name': 'event_count',
        'Type': Type.INTEGER,
        'Value': 5
    }]
    return fields == expected_fields


def test_block_aggregate_schema_initialization(block_aggregate_schema_spec, store_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(block_aggregate_schema_spec)
    schema_loader.add_schema_spec(store_spec, name)
    block_aggregate_schema = BlockAggregateSchema(name, schema_loader)
    assert match_fields(block_aggregate_schema._spec['Fields'])

    loader_spec = schema_loader.get_schema_spec(name)
    assert match_fields(loader_spec['Fields'])


def test_block_aggregate_schema_with_split_initialization(block_aggregate_schema_spec, store_spec):
    block_aggregate_schema_spec['Split'] = '4 > 2'
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(block_aggregate_schema_spec)
    schema_loader.add_schema_spec(store_spec, name)
    block_aggregate_schema = BlockAggregateSchema(name, schema_loader)
    assert isinstance(block_aggregate_schema.split, Expression)
    assert match_fields(block_aggregate_schema_spec['Fields'])

    loader_spec = schema_loader.get_schema_spec(name)
    assert match_fields(loader_spec['Fields'])


def test_block_aggregate_schema_missing_split_attribute_adds_error(block_aggregate_schema_spec, store_spec):
    del block_aggregate_schema_spec[BlockAggregateSchema.ATTRIBUTE_SPLIT]

    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(block_aggregate_schema_spec)
    schema_loader.add_schema_spec(store_spec, name)
    schema = BlockAggregateSchema(name, schema_loader)

    assert 1 == len(schema.errors)
    assert isinstance(schema.errors[0], RequiredAttributeError)
    assert BlockAggregateSchema.ATTRIBUTE_SPLIT == schema.errors[0].attribute
