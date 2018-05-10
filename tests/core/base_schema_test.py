from typing import Dict, Any

import yaml
from pytest import fixture, raises

from blurr.core.base import BaseSchema, BaseSchemaCollection
from blurr.core.errors import RequiredAttributeError, EmptyAttributeError, InvalidIdentifierError, \
    InvalidExpressionError
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.schema_loader import SchemaLoader


@fixture
def schema_spec():
    return {'Name': 'TestField', 'Type': 'integer', 'When': 'True == True'}


@fixture
def invalid_schema_spec():
    return {'Name': '_TestField', 'Type': 'integer', 'When': ''}


class MockSchema(BaseSchema):
    pass


def get_test_schema(schema_spec: Dict[str, Any]) -> MockSchema:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_spec)
    return MockSchema(name, schema_loader)


def test_with_all_attributes(schema_spec: Dict[str, Any]):
    test_schema = get_test_schema(schema_spec)
    assert test_schema.name == schema_spec[BaseSchema.ATTRIBUTE_NAME]
    assert test_schema.type == schema_spec[BaseSchema.ATTRIBUTE_TYPE]
    assert isinstance(test_schema.when, Expression)
    assert test_schema.when.evaluate(EvaluationContext())


def test_with_no_attribute_when(schema_spec: Dict[str, Any]):
    del schema_spec[BaseSchema.ATTRIBUTE_WHEN]
    test_schema = get_test_schema(schema_spec)
    assert test_schema.name == schema_spec[BaseSchema.ATTRIBUTE_NAME]
    assert test_schema.type == schema_spec[BaseSchema.ATTRIBUTE_TYPE]
    assert test_schema.when is None


def test_validate_schema_spec_missing_type_and_empty_when(invalid_schema_spec: Dict[str, Any]):
    schema = get_test_schema(invalid_schema_spec)

    assert len(schema.errors) == 2
    assert isinstance(schema.errors[0], EmptyAttributeError)
    assert schema.errors[0].attribute == BaseSchema.ATTRIBUTE_WHEN
    assert isinstance(schema.errors[1], InvalidIdentifierError)
    assert schema.errors[1].attribute == BaseSchema.ATTRIBUTE_NAME


def test_build_expression_adds_error_on_invalid_expression(schema_spec: Dict[str, Any]):

    schema_spec[BaseSchema.ATTRIBUTE_WHEN] = 'a;b'
    schema = get_test_schema(schema_spec)

    assert len(schema.errors) == 1
    assert isinstance(schema.errors[0], InvalidExpressionError)
    assert schema.errors[0].attribute == BaseSchema.ATTRIBUTE_WHEN

    with raises(
            InvalidExpressionError,
            match='`When: a;b` in section `TestField` is invalid Python expression.'):
        raise schema.errors[0]


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
    schema = MockSchemaCollection(name, schema_loader, 'Fields')
    assert not schema.errors


def test_schema_collection_missing_nested_attribute_adds_error(
        schema_collection_spec: Dict[str, Any]):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_collection_spec)
    schema = MockSchemaCollection(name, schema_loader, 'MissingNested')

    assert len(schema.errors) == 1
    assert isinstance(schema.errors[0], RequiredAttributeError)
    assert schema.errors[0].attribute == 'MissingNested'


def test_schema_collection_empty_nested_attribute_adds_error(
        schema_collection_spec: Dict[str, Any]):
    del schema_collection_spec['Fields'][0]
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_collection_spec)
    schema = MockSchemaCollection(name, schema_loader, 'Fields')

    assert len(schema.errors) == 1
    assert isinstance(schema.errors[0], EmptyAttributeError)
    assert schema.errors[0].attribute == 'Fields'
