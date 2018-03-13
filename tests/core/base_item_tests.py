from typing import Dict, Any

import yaml
from pytest import mark, fixture

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import Context, EvaluationContext
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

    def snapshot(self):
        pass

    def __init__(self,
                 schema: BaseSchema,
                 evaluation_context: EvaluationContext = EvaluationContext()
                 ) -> None:
        super().__init__(schema, evaluation_context)

    def evaluate(self)-> None:
        pass

    def restore(self, snapshot) -> None:
        pass


def test_base_item_valid(schema_spec: Dict[str, Any]) -> None:
    schema = TestSchema(schema_spec)
    test_item = TestItem(schema)
    assert test_item.schema == schema
    assert test_item.global_context == Context()
    assert test_item.local_context == Context()

    assert test_item.needs_evaluation


def test_base_item_filter_false(schema_spec: Dict[str, Any]) -> None:
    schema_spec[BaseSchema.ATTRIBUTE_WHEN] = 'False'
    schema = TestSchema(schema_spec)
    test_item = TestItem(schema)

    assert not test_item.needs_evaluation


def test_base_item_filter_missing(schema_spec: Dict[str, Any]) -> None:
    del schema_spec[BaseSchema.ATTRIBUTE_WHEN]
    schema = TestSchema(schema_spec)
    test_item = TestItem(schema)

    assert test_item.needs_evaluation
