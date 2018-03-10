from typing import Dict, Any

import yaml
from pytest import mark, fixture

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import Context
from tests.core.base_schema_tests import TestSchema


@fixture
def schema_spec() -> Dict[str, Any]:
    return yaml.load('''
Name: TestField
Type: integer
When: True == True
''')


@mark.skip(reason='Abstract base class implementation for testing')
class TestItem(BaseItem):
    """
    This class is to test abstract behavior, and thus, adds no functionality
    """

    def export(self):
        pass

    def __init__(self,
                 schema: BaseSchema,
                 global_context: Context = Context(),
                 local_context: Context = Context()) -> None:
        super().__init__(schema, global_context, local_context)

    def evaluate(self):
        pass


def test_base_item_valid(schema_spec: Dict[str, Any]) -> None:
    schema = TestSchema(schema_spec)
    test_item = TestItem(schema)
    assert test_item.schema == schema
    assert test_item.global_context == Context()
    assert test_item.local_context == Context()

    assert test_item.needs_evaluation


def test_base_item_filter_false(schema_spec: Dict[str, Any]) -> None:
    schema_spec[BaseSchema.FIELD_WHEN] = 'False'
    schema = TestSchema(schema_spec)
    test_item = TestItem(schema)

    assert not test_item.needs_evaluation


def test_base_item_filter_missing(schema_spec: Dict[str, Any]) -> None:
    del schema_spec[BaseSchema.FIELD_WHEN]
    schema = TestSchema(schema_spec)
    test_item = TestItem(schema)

    assert test_item.needs_evaluation
