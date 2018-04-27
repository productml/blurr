from typing import Dict, Any

import yaml
from pytest import fixture

from blurr.core.errors import RequiredAttributeError
from blurr.core.field import FieldSchema
from blurr.core.schema_loader import SchemaLoader


@fixture
def field_schema_spec() -> Dict[str, Any]:
    return yaml.load('''
Name: max_attempts
Type: integer
Value: 5
''')


class MockFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return int

    @property
    def default(self) -> Any:
        return int(0)


def get_mock_field_schema(schema_spec: Dict[str, Any]) -> MockFieldSchema:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_spec)
    return MockFieldSchema(name, schema_loader)


def test_field_schema_type_object(field_schema_spec):
    valid_field_schema = get_mock_field_schema(field_schema_spec)

    assert valid_field_schema.type_object == int


def test_field_schema_default_value(field_schema_spec):
    valid_field_schema = get_mock_field_schema(field_schema_spec)

    assert valid_field_schema.default == 0


def test_field_schema_is_type_of(field_schema_spec):
    valid_field_schema = get_mock_field_schema(field_schema_spec)

    assert valid_field_schema.is_type_of("Hello") == False
    assert valid_field_schema.is_type_of(1) == True


def test_field_schema_missing_value_attribute_adds_error(field_schema_spec):
    del field_schema_spec[FieldSchema.ATTRIBUTE_VALUE]
    schema = get_mock_field_schema(field_schema_spec)

    assert len(schema._errors.errors) == 1
    error = schema._errors.errors[0]
    assert isinstance(error, RequiredAttributeError)
    assert error.attribute == FieldSchema.ATTRIBUTE_VALUE
