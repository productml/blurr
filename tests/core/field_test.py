from typing import Dict, Any

import yaml
import pytest
from pytest import mark, fixture

from blurr.core.field import FieldSchema, Field
from blurr.core.errors import InvalidSchemaError
from blurr.core.evaluation import Expression
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader

@fixture
def test_field_schema() -> Dict[str, Any]:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(yaml.load('''
Name: max_attempts
Type: integer
Value: 5
'''))
    return MockFieldSchema(name, schema_loader)


class MockFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return int

    @property
    def default(self) -> Any:
        return int(0)


class MockField(Field):
    pass


def test_field_initialization(test_field_schema):
    field = MockField(test_field_schema, EvaluationContext())
    assert field.value == 0


def test_field_evaluate_with_needs_evaluation(test_field_schema):
    field = MockField(test_field_schema, EvaluationContext())
    field.evaluate()

    assert field.value == 5


def test_field_evaluate_without_needs_evaluation():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(yaml.load('''
Name: max_attempts
Type: integer
Value: 5
When: 2 == 3
'''))
    field_schema = MockFieldSchema(name, schema_loader)
    field = MockField(field_schema, EvaluationContext())
    field.evaluate()

    assert field.value == 0


def test_field_evaluate_incorrect_type():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(yaml.load('''
Name: max_attempts
Type: integer
Value: '"Hi"'
'''))
    field_schema = MockFieldSchema(name, schema_loader)
    field = MockField(field_schema, EvaluationContext())
    with pytest.raises(
            TypeError,
            match=
            "Value expression for max_attempts returned an incompatible type."
    ):
        field.evaluate()


def test_field_snapshot(test_field_schema):
    field = MockField(test_field_schema, EvaluationContext())
    assert field.snapshot == 0
    
    field.evaluate()
    assert field.snapshot == 5


def test_field_restore(test_field_schema):
    field = MockField(test_field_schema, EvaluationContext())
    field.restore(5)
    assert field.value == 5