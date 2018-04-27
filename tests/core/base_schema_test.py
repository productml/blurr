from typing import Dict, Any

import yaml
from pytest import fixture, raises

from blurr.core.base import BaseSchema, BaseSchemaCollection
from blurr.core.errors import InvalidSchemaError
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.schema_loader import SchemaLoader


@fixture
def schema_spec():
    return yaml.load('''
        Name: TestField
        Type: integer
        When: True == True
        ''')


class MockSchema(BaseSchema):
    pass


def get_test_schema(schema_spec: Dict[str, Any]) -> MockSchema:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_spec)
    return MockSchema(name, schema_loader)


def test_base_schema_with_all_attributes(schema_spec: Dict[str, Any]):
    test_schema = get_test_schema(schema_spec)
    assert test_schema.name == schema_spec[BaseSchema.ATTRIBUTE_NAME]
    assert test_schema.type == schema_spec[BaseSchema.ATTRIBUTE_TYPE]
    assert isinstance(test_schema.when, Expression)
    assert test_schema.when.evaluate(EvaluationContext())


def test_base_schema_with_no_attribute_when(schema_spec: Dict[str, Any]):
    del schema_spec[BaseSchema.ATTRIBUTE_WHEN]
    test_schema = get_test_schema(schema_spec)
    assert test_schema.name == schema_spec[BaseSchema.ATTRIBUTE_NAME]
    assert test_schema.type == schema_spec[BaseSchema.ATTRIBUTE_TYPE]
    assert test_schema.when is None


@fixture
def schema_collection_spec():
    return yaml.load('''
        Name: TestAggregate
        Type: Aggregate
        When: True == True
        Fields:
            - Name: TestField
              Type: integer
              Value: 1
        ''')


class MockSchemaCollection(BaseSchemaCollection):
    pass


def test_schema_collection_valid(schema_collection_spec: Dict[str, Any]):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_collection_spec)
    assert MockSchemaCollection(name, schema_loader, 'Fields')


def test_schema_collection_missing_nested_attribute_raises_error(
        schema_collection_spec: Dict[str, Any]):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_collection_spec)

    with raises(InvalidSchemaError, match='`MissingNested:` missing in section `{}`'.format(name)):
        return MockSchemaCollection(name, schema_loader, 'MissingNested')


def test_schema_collection_empty_nested_attribute_raises_error(
        schema_collection_spec: Dict[str, Any]):
    schema_loader = SchemaLoader()
    del schema_collection_spec['Fields'][0]

    with raises(
            InvalidSchemaError,
            match='`Fields:` in section `{}` cannot have an empty value.'.format(
                schema_collection_spec['Name'])):
        schema_loader.add_schema_spec(schema_collection_spec)
