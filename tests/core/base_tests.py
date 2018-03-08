from typing import Dict, Any
from pytest import fixture, raises
from blurr.core.base import BaseSchema
import yaml

TEST_SCHEMA = '''
Name: TestField
Type: integer
Filter: True == True
'''


class TestSchema(BaseSchema):
    def __init__(self, schema: dict):
        super().__init__(schema)


@fixture(scope='module')
def field_definitions() -> Dict[str, Any]:
    return yaml.load(TEST_SCHEMA)


@fixture(scope='module')
def field_schema(field_definitions: Dict[str, Any]) -> TestSchema:
    return TestSchema(field_definitions)


def test_base_schema_complete(field_schema: TestSchema,
                              field_definitions: Dict[str, Any]) -> None:
    assert field_schema.name == field_definitions['Name']
    assert field_schema.type == field_definitions['Type']
    assert field_schema.filter == field_definitions['Filter']
    assert eval(field_schema.filter_expr)


def test_base_schema_empty() -> None:
    with raises(KeyError, Message='Name is required for an item'):
        TestSchema({})


def test_base_schema_type_missing() -> None:
    with raises(KeyError, Message='Type is required for an item'):
        TestSchema({'Name': 'Test'})


def test_base_schema_invalid_filter() -> None:
    with raises(SyntaxError):
        TestSchema({
            'Name': 'TestName',
            'Type': 'TestType',
            'Filter': '(#&*@#$#'
        })
