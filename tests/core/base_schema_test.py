from typing import Dict, Any

import yaml
from pytest import fixture, mark

from blurr.core.base import BaseSchema
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.schema_loader import SchemaLoader

TEST_SCHEMA_SPEC = '''
Name: TestField
Type: integer
When: True == True
'''


@fixture
def test_schema_spec():
    return yaml.load(TEST_SCHEMA_SPEC)


@mark.skip(reason='Abstract base class implementation for testing')
class TestSchema(BaseSchema):
    """
    This class is to test abstract behavior, and thus, adds no functionality
    """

    def load(self) -> None:
        pass

    def validate(self, spec: Dict[str, Any]):
        pass


def get_test_schema(schema_spec: Dict[str, Any]) -> TestSchema:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(schema_spec)
    return TestSchema(name, schema_loader)


def test_base_schema_with_all_attributes(
        test_schema_spec: Dict[str, Any]) -> None:
    test_schema = get_test_schema(test_schema_spec)
    assert test_schema.name == test_schema_spec[BaseSchema.ATTRIBUTE_NAME]
    assert test_schema.type == test_schema_spec[BaseSchema.ATTRIBUTE_TYPE]
    assert isinstance(test_schema.when, Expression)
    assert test_schema.when.evaluate(EvaluationContext())


def test_base_schema_with_no_attribute_when(
        test_schema_spec: Dict[str, Any]) -> None:
    del test_schema_spec[BaseSchema.ATTRIBUTE_WHEN]
    test_schema = get_test_schema(test_schema_spec)
    assert test_schema.name == test_schema_spec[BaseSchema.ATTRIBUTE_NAME]
    assert test_schema.type == test_schema_spec[BaseSchema.ATTRIBUTE_TYPE]
    assert test_schema.when is None
