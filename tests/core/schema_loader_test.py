from typing import Dict

import pytest
from pytest import fixture

from blurr.core.type import Type
from blurr.core.errors import InvalidSchemaError
from blurr.core.schema_loader import SchemaLoader
from blurr.core.field_simple import IntegerFieldSchema
from blurr.core.transformer_streaming import StreamingTransformerSchema


@fixture
def nested_schema_spec_bad_type() -> Dict:
    return {
        'Name': 'test',
        'Type': 'Blurr:Unknown',
        'Ignored': 2,
        'Aggregates': [{
            'Name': 'test_group',
            'Fields': [{
                "Type": "string",
                "Name": "country",
                "Value": "source.country"
            }, {
                "Type": "integer",
                "Name": "events",
                "Value": "test_group.events+1"
            }]
        }]
    }


@fixture
def nested_schema_spec() -> Dict:
    return {
        'Name': 'test',
        'Type': Type.BLURR_TRANSFORM_STREAMING,
        "Version": "2018-03-01",
        "Time": "parser.parse(source.event_time)",
        "Identity": "source.user_id",
        'Ignored': 2,
        'Aggregates': [{
            'Name': 'test_group',
            'Type': Type.BLURR_AGGREGATE_IDENTITY,
            'Fields': [{
                "Type": "string",
                "Name": "country",
                "Value": "source.country"
            }, {
                "Type": "integer",
                "Name": "events",
                "Value": "test_group.events+1"
            }]
        }]
    }


@fixture
def schema_loader(nested_schema_spec) -> SchemaLoader:
    schema_loader = SchemaLoader()
    schema_loader.add_schema(nested_schema_spec)
    return schema_loader


def test_add_invalid_schema() -> None:
    schema_loader = SchemaLoader()

    assert schema_loader.add_schema('') is None
    assert schema_loader.add_schema(['test']) is None
    assert schema_loader.add_schema({'test': 1}) is None


def test_add_valid_simple_schema() -> None:
    schema_loader = SchemaLoader()

    assert schema_loader.add_schema({'Name': 'test'}) == 'test'
    assert schema_loader.get_schema_spec('test') == {'Name': 'test'}


def test_add_valid_simple_schema_with_parent() -> None:
    schema_loader = SchemaLoader()

    assert schema_loader.add_schema({'Name': 'test'}, 'parent') == 'test'
    assert schema_loader.get_schema_spec('parent.test') == {'Name': 'test'}


def test_add_valid_nested_schema(nested_schema_spec_bad_type: Dict) -> None:
    schema_loader = SchemaLoader()

    assert schema_loader.add_schema(nested_schema_spec_bad_type) == 'test'
    assert schema_loader.get_schema_spec('test.test_group') == nested_schema_spec_bad_type[
        'Aggregates'][0]
    assert schema_loader.get_schema_spec('test.test_group.country') == nested_schema_spec_bad_type[
        'Aggregates'][0]['Fields'][0]
    assert schema_loader.get_schema_spec('test.test_group.events') == nested_schema_spec_bad_type[
        'Aggregates'][0]['Fields'][1]


def test_get_schema_object_error(nested_schema_spec_bad_type: Dict) -> None:
    schema_loader = SchemaLoader()
    schema_loader.add_schema(nested_schema_spec_bad_type)

    with pytest.raises(InvalidSchemaError, match='Unknown schema type Blurr:Unknown'):
        schema_loader.get_schema_object('test')

    with pytest.raises(InvalidSchemaError, match='Type not defined in schema'):
        schema_loader.get_schema_object('test.test_group')


def test_get_schema_object(schema_loader: SchemaLoader) -> None:
    assert isinstance(schema_loader.get_schema_object('test'), StreamingTransformerSchema) is True
    field_schema = schema_loader.get_schema_object('test.test_group.events')
    assert isinstance(field_schema, IntegerFieldSchema) is True

    # Assert that the same object is returned and a new one is not created.
    assert field_schema.when is None
    field_schema.when = 'True'
    assert schema_loader.get_schema_object('test.test_group.events').when == 'True'


def test_get_nested_schema_object(schema_loader: SchemaLoader):
    assert isinstance(
        schema_loader.get_nested_schema_object('test.test_group', 'events'),
        IntegerFieldSchema) is True


def test_get_fully_qualified_name() -> None:
    assert SchemaLoader.get_fully_qualified_name('parent', 'child') == 'parent.child'


def test_get_schemas_of_type(schema_loader: SchemaLoader, nested_schema_spec: Dict) -> None:
    assert schema_loader.get_schemas_of_type(
        [Type.INTEGER]) == [('test.test_group.events',
                             nested_schema_spec['Aggregates'][0]['Fields'][1])]


def test_get_transformer_name() -> None:
    assert SchemaLoader.get_transformer_name('test.child1.child2') == 'test'
