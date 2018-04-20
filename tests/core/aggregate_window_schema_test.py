from pytest import raises

from blurr.core.aggregate_block import BlockAggregateSchema
from blurr.core.constants import BLURR_AGGREGATE_BLOCK, BLURR_AGGREGATE_WINDOW
from blurr.core.errors import InvalidSchemaError
from blurr.core.schema_loader import SchemaLoader
from blurr.core.aggregate_window import WindowAggregateSchema


def test_initialization_with_valid_source(schema_loader_with_mem_store: SchemaLoader,
                                          mem_store_name: str, stream_dtc_name: str):
    schema_loader_with_mem_store.add_schema({
        'Type': BLURR_AGGREGATE_BLOCK,
        'Name': 'session',
        'Store': mem_store_name,
        'Fields': [
            {
                'Name': 'events',
                'Type': 'integer',
                'Value': 'session.events + 1',
            },
        ],
    }, stream_dtc_name)
    name = schema_loader_with_mem_store.add_schema({
        'Type': BLURR_AGGREGATE_WINDOW,
        'Name': 'test_window_name',
        'WindowType': 'day',
        'WindowValue': 1,
        'Source': stream_dtc_name + '.session',
        'Fields': [{
            'Name': 'total_events',
            'Type': 'integer',
            'Value': 'sum(source.events)'
        }]
    })

    window_aggregate_schema = WindowAggregateSchema(name, schema_loader_with_mem_store)
    assert window_aggregate_schema.window_type == 'day'
    assert window_aggregate_schema.window_value == 1
    assert isinstance(window_aggregate_schema.source, BlockAggregateSchema)
    assert window_aggregate_schema.source.name == 'session'


def test_initialization_with_invalid_source(schema_loader_with_mem_store: SchemaLoader,
                                            stream_dtc_name: str):

    name = schema_loader_with_mem_store.add_schema({
        'Type': BLURR_AGGREGATE_WINDOW,
        'Name': 'test_window_name',
        'WindowType': 'day',
        'WindowValue': 1,
        'Source': stream_dtc_name + '.session',
        'Fields': [{
            'Name': 'total_events',
            'Type': 'integer',
            'Value': 'sum(source.events)'
        }]
    })

    with raises(InvalidSchemaError, match=stream_dtc_name + '.session not declared in schema'):
        WindowAggregateSchema(name, schema_loader_with_mem_store)
