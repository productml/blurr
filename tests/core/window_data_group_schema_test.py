from blurr.core.schema_loader import SchemaLoader
from blurr.core.window_data_group import WindowDataGroupSchema
from blurr.core.session_data_group import SessionDataGroupSchema
from blurr.core.errors import InvalidSchemaError
from pytest import fixture, raises


def test_initialization_with_valid_source(
        schema_loader_with_mem_store: SchemaLoader, mem_store_name: str,
        stream_dtc_name: str):
    schema_loader_with_mem_store.add_schema({
        'Type': 'ProductML:DTC:DataGroup:SessionAggregate',
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
        'Type': 'ProductML:DTC:DataGroup:WindowAggregate',
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

    window_data_group_schema = WindowDataGroupSchema(
        name, schema_loader_with_mem_store)
    assert window_data_group_schema.window_type == 'day'
    assert window_data_group_schema.window_value == 1
    assert isinstance(window_data_group_schema.source, SessionDataGroupSchema)
    assert window_data_group_schema.source.name == 'session'


def test_initialization_with_invalid_source(
        schema_loader_with_mem_store: SchemaLoader, mem_store_name: str,
        stream_dtc_name: str):

    name = schema_loader_with_mem_store.add_schema({
        'Type': 'ProductML:DTC:DataGroup:WindowAggregate',
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

    with raises(
            InvalidSchemaError,
            match=stream_dtc_name + '.session not declared in schema'):
        WindowDataGroupSchema(name, schema_loader_with_mem_store)
