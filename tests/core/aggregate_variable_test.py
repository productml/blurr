from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate_variable import VariableAggregateSchema
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type
from blurr.core.validator import ATTRIBUTE_INTERNAL


@fixture
def schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_VARIABLE,
        'Name': 'user',
        'Fields': [{
            'Name': 'event_count',
            'Type': Type.INTEGER,
            'Value': 5
        }]
    }


def match_fields(fields):
    expected_fields = [{
        'Name': '_identity',
        'Type': Type.STRING,
        'Value': 'identity',
        ATTRIBUTE_INTERNAL: True
    }, {
    #     'Name': '_processed_tracker',
    #     'Type': Type.BLOOM_FILTER,
    #     'Value': 'user._processed_tracker.add(time.isoformat())',
    #     ATTRIBUTE_INTERNAL: True
    # }, {
        'Name': 'event_count',
        'Type': Type.INTEGER,
        'Value': 5
    }]
    return fields == expected_fields


def test_variable_aggregate_initialization(schema_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_spec)
    schema = VariableAggregateSchema(name, schema_loader)
    assert match_fields(schema._spec['Fields'])

    loader_spec = schema_loader.get_schema_spec(name)
    assert match_fields(loader_spec['Fields'])
