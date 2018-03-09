from typing import Dict, Any
from pytest import fixture, raises, mark
from blurr.core.base import BaseSchema, Expression
from blurr.core.errors import InvalidSchemaException
import yaml

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

    def __init__(self, spec: Dict[str, Any]):
        super().__init__(spec)

    def validate(self, spec: Dict[str, Any]):
        pass

    def load(self, spec: Dict[str, Any]):
        pass


def test_base_schema_valid(test_schema_spec: Dict[str, Any]) -> None:
    test_schema = TestSchema(test_schema_spec)
    assert test_schema.name == test_schema_spec[BaseSchema.FIELD_NAME]
    assert test_schema.type == test_schema_spec[BaseSchema.FIELD_TYPE]
    assert isinstance(test_schema.when, Expression)
    assert test_schema.when.evaluate()


def test_base_schema_empty() -> None:
    with raises(InvalidSchemaException, Message='Required attribute missing.'):
        TestSchema({})


def test_base_schema_name_missing(test_schema_spec: Dict[str, Any]) -> None:
    del test_schema_spec[BaseSchema.FIELD_NAME]
    with raises(InvalidSchemaException, Message='Required attribute missing.'):
        TestSchema(test_schema_spec)


def test_base_schema_type_missing(test_schema_spec: Dict[str, Any]) -> None:
    del test_schema_spec[BaseSchema.FIELD_TYPE]
    with raises(InvalidSchemaException, Message='Required attribute missing.'):
        TestSchema(test_schema_spec)


def test_base_schema_invalid_filter(test_schema_spec: Dict[str, Any]) -> None:
    test_schema_spec[BaseSchema.FIELD_WHEN] = '(#&*@#$#'
    with raises(SyntaxError):
        TestSchema(test_schema_spec)
