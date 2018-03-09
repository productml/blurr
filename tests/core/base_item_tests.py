from typing import Dict, Any

from pytest import mark, fixture

from blurr.core.base import BaseSchema, Expression, BaseItem, EvaluationResult
from blurr.core.context import Context
from tests.core.base_schema_tests import TestSchema
import yaml


@fixture
def test_schema_spec() -> Dict[str, Any]:
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

    def __init__(self, schema: BaseSchema, global_context: Context = Context(),
                 local_context: Context = Context()) -> None:
        super().__init__(schema, global_context, local_context)

    def evaluate_item(self) -> EvaluationResult:
        return EvaluationResult(True)


def test_base_item_valid(test_schema_spec: Dict[str, Any]) -> None:
    schema = TestSchema(test_schema_spec)
    test_item = TestItem(schema)
    assert test_item.schema == schema
    assert test_item.global_context == Context()
    assert test_item.local_context == Context()

    result = test_item.evaluate()
    assert isinstance(result, EvaluationResult)
    assert result.success
    assert result.result
    assert not result.skip_cause
    assert not result.error


def test_base_item_filter_false(test_schema_spec: Dict[str, Any]) -> None:
    test_schema_spec[BaseSchema.FIELD_WHEN] = 'False'
    schema = TestSchema(test_schema_spec)
    test_item = TestItem(schema)

    result = test_item.evaluate()
    assert not result.success
    assert result.result is None
    assert result.skip_cause == 'When condition evaluated to False'
    assert result.error is None


def test_base_item_filter_invalid(test_schema_spec: Dict[str, Any]) -> None:
    test_schema_spec[BaseSchema.FIELD_WHEN] = '9292#?&@&^'
    schema = TestSchema(test_schema_spec)
    test_item = TestItem(schema)

    result = test_item.evaluate()
    assert not result.success
    assert result.result is None
    assert result.skip_cause == 'Error occurred'
    assert isinstance(result.error, Exception)
