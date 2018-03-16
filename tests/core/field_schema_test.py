from typing import Dict, Any

import yaml
import pytest
from pytest import mark, fixture

from blurr.core.field import FieldSchema
from blurr.core.errors import InvalidSchemaError
from blurr.core.evaluation import Expression
from blurr.core.evaluation import EvaluationContext


@fixture
def field_schema_spec() -> Dict[str, Any]:
    return yaml.load('''
Name: max_attempts
Type: integer
Value: 5
''')


class MockFieldSchema(FieldSchema):
    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)

    @property
    def type_object(self) -> Any:
        return int

    @property
    def default(self) -> Any:
        return int(0)


def test_field_schema_missing_required_parameter():
    missing_field_spec = yaml.load('''
Name: events
Type: integer
''')
    with pytest.raises(
            InvalidSchemaError, match="Required attribute missing."):
        MockFieldSchema(missing_field_spec)


def test_field_schema_valid_schema(field_schema_spec):
    valid_field_schema = MockFieldSchema(field_schema_spec)

    assert valid_field_schema.name == "max_attempts"
    assert valid_field_schema.type == "integer"
    assert valid_field_schema.value.evaluate(EvaluationContext()) == 5


def test_field_schema_type_object(field_schema_spec):
    valid_field_schema = MockFieldSchema(field_schema_spec)

    assert valid_field_schema.type_object == int


def test_field_schema_default_value(field_schema_spec):
    valid_field_schema = MockFieldSchema(field_schema_spec)

    assert valid_field_schema.default == 0


def test_field_schema_is_type_of(field_schema_spec):
    valid_field_schema = MockFieldSchema(field_schema_spec)

    assert valid_field_schema.is_type_of("Hello") == False
    assert valid_field_schema.is_type_of(1) == True
