from typing import Dict, Any

import yaml
from pytest import fixture

from blurr.core.base import BaseSchema, BaseSchemaCollection
from blurr.core.errors import RequiredAttributeError, EmptyAttributeError
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
    def validate_schema_spec(self) -> None:
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
    schema = MockSchemaCollection(name, schema_loader, 'Fields')
    assert not schema.errors


def test_schema_collection_missing_nested_attribute_adds_error(
        schema_collection_spec: Dict[str, Any]):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_collection_spec)
    schema = MockSchemaCollection(name, schema_loader, 'MissingNested')

    assert 1 == len(schema.errors)
    assert isinstance(schema.errors[0], RequiredAttributeError)
    assert 'MissingNested' == schema.errors[0].attribute


def test_schema_collection_empty_nested_attribute_adds_error(schema_collection_spec: Dict[str, Any]):
    del schema_collection_spec['Fields'][0]
    schema_loader = SchemaLoader()
    schema_loader.add_schema_spec(schema_collection_spec)

    assert len(schema_loader._errors.errors) == 1
    error = schema_loader._errors.errors[0]
    assert isinstance(error, EmptyAttributeError)
    assert error.attribute == 'Fields'
