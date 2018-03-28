from typing import Dict, Any

from pytest import fixture, raises

from blurr.core.evaluation import EvaluationContext
from blurr.core.set_field import Set
from blurr.core.schema_loader import SchemaLoader
from blurr.core.simple_field import SimpleField


# Test the functions of the Set object

def test_set_add():
    sample = Set()

    assert 'item' not in sample
    assert sample.add('item') is sample
    assert 'item' in sample


def test_set_remove():
    sample = Set([True, False])

    assert sample.remove(True) is sample
    assert len(sample) == 1
    assert True not in sample
    assert False in sample

    with raises(KeyError):
        sample.remove(1)

    assert len(sample) == 1


def test_set_discard():
    sample = Set([True, False])

    assert sample.discard(True) is sample
    assert len(sample) == 1
    assert True not in sample
    assert False in sample
    assert sample.discard(1) is sample
    assert len(sample) == 1


def test_set_clear():
    sample = Set({True, False})

    assert sample.clear() is sample
    assert len(sample) == 0


def test_set_copy():
    sample = Set({True, False})
    sample_copy = sample.copy()

    assert sample is not sample_copy
    assert sample == sample_copy


def test_set_difference():
    sample = Set({True, False})
    result = sample.difference({True})
    assert isinstance(result, Set)
    assert result is not sample
    assert result == {False}


def test_set_difference_update():
    sample = Set({True, False})
    result = sample.difference_update({True})
    assert result is sample
    assert result == {False}


def test_set_intersection():
    sample = Set({True, False})
    result = sample.intersection({True})
    assert isinstance(result, Set)
    assert result is not sample
    assert result == {True}


def test_set_intersection_update():
    sample = Set({True, False})
    result = sample.intersection_update({True})
    assert result is sample
    assert result == {True}


def test_set_symmetric_difference():
    sample = Set({1, 2})
    result = sample.symmetric_difference({2, 3})
    assert isinstance(result, Set)
    assert result is not sample
    assert result == {1, 3}


def test_set_symmetric_difference_update():
    sample = Set({1, 2})
    result = sample.symmetric_difference_update({2, 3})
    assert result is sample
    assert result == {1, 3}


def test_set_union():
    sample = Set({1, 2})
    result = sample.union({2, 3})
    assert isinstance(result, Set)
    assert result is not sample
    assert result == {1, 2, 3}


def test_set_update():
    sample = Set({1, 2})
    result = sample.update({2, 3})
    assert result is sample
    assert result == {1, 2, 3}


# Test the List Field Evaluation in isolation

@fixture(scope='module')
def field_schema_spec() -> Dict[str, Any]:
    return {
        'Name': 'set_field',
        'Type': 'set',
        'Value': 'set_field.add(1).copy().union({2, 3}).update({3, 4, 5}).discard(0).remove(1).intersection({2, 4, 5}).symmetric_difference_update({4, 6})'
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
    context.global_add('set_field', field.value)

    initial_value = field.value
    assert len(initial_value) == 0

    field.evaluate()

    assert field.value is not initial_value
    assert len(field.value) == 3
    assert field.value == {2, 5, 6}
