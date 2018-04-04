from typing import Dict, Any

import yaml
from pytest import mark, fixture

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from tests.core.base_schema_test import TestSchema


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

    def _snapshot(self):
        pass

    def evaluate(self) -> None:
        pass

    def restore(self, snapshot) -> None:
        pass


def get_test_item(schema_spec: Dict[str, Any]) -> TestItem:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(schema_spec)
    schema = TestSchema(name, schema_loader)
    return TestItem(schema, EvaluationContext())


def test_base_item_valid(schema_spec: Dict[str, Any]) -> None:
    test_item = get_test_item(schema_spec)
    assert test_item._schema.name == 'TestField'
    assert test_item._schema.type == 'integer'
    assert len(test_item._evaluation_context.global_context) == 0
    assert len(test_item._evaluation_context.local_context) == 0

    assert test_item._needs_evaluation


def test_base_item_filter_false(schema_spec: Dict[str, Any]) -> None:
    schema_spec[BaseSchema.ATTRIBUTE_WHEN] = 'False'
    test_item = get_test_item(schema_spec)

    assert not test_item._needs_evaluation


def test_base_item_filter_missing(schema_spec: Dict[str, Any]) -> None:
    del schema_spec[BaseSchema.ATTRIBUTE_WHEN]
    test_item = get_test_item(schema_spec)

    assert test_item._needs_evaluation
