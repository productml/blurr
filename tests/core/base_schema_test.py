from typing import Dict, Any

import yaml
from pytest import fixture, raises

from blurr.core.base import BaseSchema
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
