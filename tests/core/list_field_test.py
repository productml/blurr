from typing import Dict, Any

from pytest import fixture

from blurr.core.evaluation import EvaluationContext
from blurr.core.complex_fields import List
from blurr.core.schema_loader import SchemaLoader
from blurr.core.simple_fields import SimpleField

# Test the functions of the List object


def test_list_append():
    sample = List()

    assert 'item' not in sample
    assert sample.append('item') is sample
    assert 'item' in sample


def test_list_insert():
    sample = List([True])

    assert sample[0] is True
    assert sample.insert(0, False) is sample
    assert len(sample) == 2
    assert sample[0] is False


def test_list_extend():
    sample = List()

    assert sample.extend([True, False]) is sample
    assert len(sample) == 2
    assert True in sample
    assert False in sample


def test_list_remove():
    sample = List([True, False])

    assert sample.remove(True) is sample
    assert len(sample) == 1
    assert True not in sample
    assert False in sample


def test_list_clear():
    sample = List([True, False])

    assert sample.clear() is sample
    assert len(sample) == 0


def test_list_copy():
    sample = List([True, False])
    sample_copy = sample.copy()

    assert sample is not sample_copy
    assert sample == sample_copy


# Test the List Field Evaluation in isolation


@fixture(scope='module')
def field_schema_spec() -> Dict[str, Any]:
    return {
        'Name': 'list_field',
        'Type': 'list',
        'Value': 'list_field.append(1).insert(0, 0).extend([2, 3]).remove(3)'
    }


@fixture(scope='module')
def schema_loader() -> SchemaLoader:
    return SchemaLoader()


@fixture(scope='module')
def field_schema(schema_loader, field_schema_spec):
    return schema_loader.add_schema(field_schema_spec)


def test_map_field_evaluation(schema_loader, field_schema):
    context = EvaluationContext()
    field = SimpleField(schema_loader.get_schema_object(field_schema), context)
    context.global_add('list_field', field.value)

    initial_value = field.value
    assert len(initial_value) == 0

    field.evaluate()

    assert field.value is initial_value
    assert len(field.value) == 3
    assert field.value == [0, 1, 2]
