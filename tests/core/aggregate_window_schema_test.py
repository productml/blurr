from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate_block import BlockAggregateSchema
from blurr.core.aggregate_window import WindowAggregateSchema
from blurr.core.errors import RequiredAttributeError
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type


@fixture
def aggregate_block_schema_spec(mem_store_name: str) -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_BLOCK,
        'Name': 'session',
        'Store': mem_store_name,
        'Fields': [
            {
                'Name': 'events',
                'Type': Type.INTEGER,
                'Value': 'session.events + 1',
            },
        ],
    }


@fixture
def window_schema_spec(stream_dtc_name: str) -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_WINDOW,
        'Name': 'test_window_name',
        'WindowType': Type.DAY,
        'WindowValue': 1,
        'Source': stream_dtc_name + '.session',
        'Fields': [{
            'Name': 'total_events',
            'Type': Type.INTEGER,
            'Value': 'sum(source.events)'
        }]
    }


def test_initialization_with_valid_source(schema_loader_with_mem_store: SchemaLoader,
                                          aggregate_block_schema_spec: Dict[str, Any],
                                          window_schema_spec: Dict[str, Any], stream_dtc_name: str):
    schema_loader_with_mem_store.add_schema_spec(aggregate_block_schema_spec, stream_dtc_name)
    name = schema_loader_with_mem_store.add_schema_spec(window_schema_spec)

    window_aggregate_schema = WindowAggregateSchema(name, schema_loader_with_mem_store)
    assert Type.is_type_equal(window_aggregate_schema.window_type, Type.DAY)
    assert window_aggregate_schema.window_value == 1
    assert isinstance(window_aggregate_schema.source, BlockAggregateSchema)
    assert window_aggregate_schema.source.name == 'session'


def test_initialization_with_invalid_source(schema_loader_with_mem_store: SchemaLoader,
                                            window_schema_spec: Dict[str, Any]):
    name = schema_loader_with_mem_store.add_schema_spec(window_schema_spec)

    schema = WindowAggregateSchema(name, schema_loader_with_mem_store)
    assert len(schema.errors) == 0
    assert len(schema_loader_with_mem_store.get_errors()) == 1


def test_window_aggregate_schema_missing_attributes_adds_error(
        schema_loader_with_mem_store: SchemaLoader, aggregate_block_schema_spec: Dict[str, Any],
        window_schema_spec: Dict[str, Any], stream_dtc_name: str):
    del window_schema_spec[WindowAggregateSchema.ATTRIBUTE_WINDOW_TYPE]
    del window_schema_spec[WindowAggregateSchema.ATTRIBUTE_WINDOW_VALUE]
    del window_schema_spec[WindowAggregateSchema.ATTRIBUTE_SOURCE]

    schema_loader_with_mem_store.add_schema_spec(aggregate_block_schema_spec, stream_dtc_name)
    name = schema_loader_with_mem_store.add_schema_spec(window_schema_spec)

    schema = WindowAggregateSchema(name, schema_loader_with_mem_store)

    assert 3 == len(schema.errors)
    assert RequiredAttributeError(name, {}, WindowAggregateSchema.ATTRIBUTE_WINDOW_TYPE) in schema.errors
    assert RequiredAttributeError(name, {}, WindowAggregateSchema.ATTRIBUTE_WINDOW_VALUE) in schema.errors
    assert RequiredAttributeError(name, {}, WindowAggregateSchema.ATTRIBUTE_SOURCE) in schema.errors
