from typing import Dict, Any

import pytest
from pytest import fixture

from blurr.core.evaluation import EvaluationContext
from blurr.core.field import FieldSchema, Field
from blurr.core.schema_loader import SchemaLoader
from blurr.core.simple_fields import SimpleField, BooleanFieldSchema, IntegerFieldSchema, FloatFieldSchema


@fixture
def test_field_schema() -> Dict[str, Any]:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'integer', 'Value': 5})
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
    name = schema_loader.add_schema({
        'Name': 'max_attempts',
        'Type': 'integer',
        'Value': 5,
        'When': '2 == 3'
    })
    field_schema = MockFieldSchema(name, schema_loader)
    field = MockField(field_schema, EvaluationContext())
    field.evaluate()

    assert field.value == 0


def test_field_evaluate_incorrect_typecast_to_type_default():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'integer', 'Value': '"Hi"'})
    field_schema = IntegerFieldSchema(name, schema_loader)
    field = SimpleField(field_schema, EvaluationContext())

    assert field.value == 0


def test_field_evaluate_implicit_typecast_integer():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'integer', 'Value': '23.45'})
    field_schema = IntegerFieldSchema(name, schema_loader)
    field = SimpleField(field_schema, EvaluationContext())
    field.evaluate()

    assert field._snapshot == 23


def test_field_evaluate_implicit_typecast_float():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'float', 'Value': '23'})
    field_schema = FloatFieldSchema(name, schema_loader)
    field = SimpleField(field_schema, EvaluationContext())
    field.evaluate()

    assert field._snapshot == 23.0


def test_field_evaluate_implicit_typecast_bool():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'boolean', 'Value': '1+2'})
    field_schema = BooleanFieldSchema(name, schema_loader)
    field = SimpleField(field_schema, EvaluationContext())
    field.evaluate()

    assert field._snapshot is True


def test_field_snapshot(test_field_schema):
    field = MockField(test_field_schema, EvaluationContext())
    assert field._snapshot == 0

    field.evaluate()
    assert field._snapshot == 5


def test_field_restore(test_field_schema):
    field = MockField(test_field_schema, EvaluationContext())
    field.restore(5)
    assert field.value == 5
