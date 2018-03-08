from typing import Dict, Any
from pytest import fixture
from core.base import BaseSchema
import yaml

FIELD_SCHEMA = '''
Name: TestField
Type: integer
Filter: True == True
'''


class TestSchema(BaseSchema):
    def __init__(self, schema: dict):
        super().__init__(schema)


@fixture(scope='module')
def field_definitions() -> Dict[str, Any]:
    return yaml.load(FIELD_SCHEMA)


@fixture(scope='module')
def field_schema(field_definitions: Dict[str, Any]) -> TestSchema:
    return TestSchema(field_definitions)


def test_base_schema(field_schema: TestSchema, field_definitions: Dict[str, Any]) -> None:
    assert field_schema.name == field_definitions['Name']
