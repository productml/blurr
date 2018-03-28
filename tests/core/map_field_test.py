from typing import Dict, Any

from pytest import fixture

from blurr.core.evaluation import EvaluationContext
from blurr.core.map_field import Map
from blurr.core.schema_loader import SchemaLoader
from blurr.core.simple_field import SimpleField


# Test the functions of the Map object

def test_map_set():
    sample = Map()

    assert 'key' not in sample
    assert sample.set('key', 'value') is sample
    assert sample.get('key') == 'value'


def test_map_increment():
    sample = Map()

    assert 'key' not in sample
    assert sample.increment('key') is sample
    assert sample['key'] == 1
    assert sample.increment('key', 100) is sample
    assert sample.get('key') == 101


def test_map_update():
    sample = Map()

    assert sample.update({'key': 'value'}) is sample
    assert len(sample) == 1
    assert sample['key'] == 'value'


def test_map_clear():
    sample = Map({'key': 'value'})

    assert len(sample) == 1
    assert 'key' in sample
    assert sample.clear() is sample
    assert len(sample) == 0


# Test the Map Field Evaluation in isolation

@fixture(scope='module')
def map_field_schema_spec() -> Dict[str, Any]:
    return {
        'Name': 'map_field',
        'Type': 'map',
        'Value': 'map_field.set("set", "value").increment("incr", 10).update({"update": "value"})'
    }


@fixture(scope='module')
def schema_loader() -> SchemaLoader:
    return SchemaLoader()


@fixture(scope='module')
def map_field_schema(schema_loader, map_field_schema_spec):
    return schema_loader.add_schema(map_field_schema_spec)


def test_map_field_evaluation(schema_loader, map_field_schema):
    context = EvaluationContext()
    field = SimpleField(schema_loader.get_schema_object(map_field_schema), context)
    context.global_add('map_field', field.value)

    initial_map = field.value
    assert len(initial_map) == 0

    field.evaluate()

    assert field.value is initial_map
    assert len(field.value) == 3
    assert field.value['incr'] == 10
    assert field.value['set'] == 'value'
    assert field.value['update'] == 'value'
