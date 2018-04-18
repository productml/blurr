import logging
from typing import Dict, Any

import pytest
from pytest import fixture

from blurr.core.evaluation import EvaluationContext
from blurr.core.field import FieldSchema, Field
from blurr.core.field_complex import SetFieldSchema
from blurr.core.schema_loader import SchemaLoader
from blurr.core.field_simple import Field, BooleanFieldSchema, IntegerFieldSchema, FloatFieldSchema


class MockFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return int

    @property
    def default(self) -> Any:
        return int(0)


@fixture
def test_field_schema() -> MockFieldSchema:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'integer', 'Value': 5})
    return MockFieldSchema(name, schema_loader)


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


def test_field_evaluate_incorrect_typecast_to_type_default(caplog):
    caplog.set_level(logging.DEBUG)
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'integer', 'Value': '"Hi"'})
    field_schema = IntegerFieldSchema(name, schema_loader)
    field = Field(field_schema, EvaluationContext())
    field.evaluate()

    assert field.value == 0
    assert ('ValueError in casting Hi to integer for field max_attempts. Error: invalid '
            'literal for int() with base 10: \'Hi\'') in caplog.records[0].message
    assert caplog.records[0].levelno == logging.DEBUG


def test_field_evaluate_implicit_typecast_integer():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'integer', 'Value': '23.45'})
    field_schema = IntegerFieldSchema(name, schema_loader)
    field = Field(field_schema, EvaluationContext())
    field.evaluate()

    assert field._snapshot == 23


def test_field_evaluate_implicit_typecast_float():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'float', 'Value': '23'})
    field_schema = FloatFieldSchema(name, schema_loader)
    field = Field(field_schema, EvaluationContext())
    field.evaluate()

    assert field._snapshot == 23.0


def test_field_evaluate_implicit_typecast_bool():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'max_attempts', 'Type': 'boolean', 'Value': '1+2'})
    field_schema = BooleanFieldSchema(name, schema_loader)
    field = Field(field_schema, EvaluationContext())
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


def test_field_reset(test_field_schema):
    field = MockField(test_field_schema, EvaluationContext())
    field.restore(5)
    assert field.value == 5
    field.reset()
    assert field.value == 0


def test_set_field_snapshot_encoding():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'test', 'Type': 'set', 'Value': 'test.add(0).add(1)'})
    field_schema = SetFieldSchema(name, schema_loader)
    field = Field(field_schema, EvaluationContext())
    field._evaluation_context.global_add('test', field.value)
    field.evaluate()

    assert field.value == {0, 1}
    assert field._snapshot
    assert isinstance(field._snapshot, list)
    assert set(field._snapshot) == field.value


def test_set_field_snapshot_decoding():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema({'Name': 'test', 'Type': 'set', 'Value': 'test.add(0).add(1)'})
    field_schema = SetFieldSchema(name, schema_loader)
    field = Field(field_schema, EvaluationContext())
    field.restore([2, 3])

    assert field.value == {2, 3}
    assert field._snapshot
    assert isinstance(field._snapshot, list)
    assert set(field._snapshot) == field.value
