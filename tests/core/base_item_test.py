import importlib
import pkgutil
import sys
from typing import Dict, Any

import yaml
from pytest import fixture

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type
from tests.core.base_schema_test import MockSchema


@fixture
def schema_spec() -> Dict[str, Any]:
    return yaml.load('''
Name: TestField
Type: integer
When: True == True
''')


class MockItem(BaseItem):
    """
    This class is to test abstract behavior, and thus, adds no functionality
    """

    def run_reset(self) -> None:
        pass

    def _snapshot(self):
        pass

    def run_evaluate(self) -> None:
        pass

    def run_restore(self, snapshot) -> None:
        pass


def get_test_item(schema_spec: Dict[str, Any]) -> MockItem:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_spec)
    schema = MockSchema(name, schema_loader)
    return MockItem(schema, EvaluationContext())


def test_base_item_valid(schema_spec: Dict[str, Any]) -> None:
    test_item = get_test_item(schema_spec)
    assert test_item._schema.name == 'TestField'
    assert Type.is_type_equal(test_item._schema.type, Type.INTEGER)
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


def test_base_item_method_naming():
    def import_submodules(package_name):
        """ Import all submodules of a module, recursively """
        package = sys.modules[package_name]
        return {
            name: importlib.import_module(package_name + '.' + name)
            for loader, name, is_pkg in pkgutil.walk_packages(package.__path__)
        }

    def inheritors(base_class):
        import_submodules('blurr')
        subclasses = set()
        work = [base_class]
        while work:
            parent = work.pop()
            for child in parent.__subclasses__():
                if child not in subclasses:
                    subclasses.add(child)
                    work.append(child)
        return subclasses

    item_classes = inheritors(BaseItem)
    fail_assert = False
    for item_class in item_classes:
        for member in dir(item_class):
            if not member.startswith('_') and not member.startswith('run_'):
                print(
                    'Class members for classes inheriting from BaseItem should start with _. Error:',
                    member, 'in Class:', item_class.__name__)
                fail_assert = True

    assert not fail_assert
