from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate import Aggregate, AggregateSchema
from blurr.core.aggregate_variable import VariableAggregate, \
    VariableAggregateSchema
from blurr.core.evaluation import EvaluationContext
from blurr.core.field_complex import Map, List, Set
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type

# Test custom functions implemented for complex fields


def test_map_set() -> None:
    sample = Map()

    assert 'key' not in sample
    assert sample.set('key', 'value') is sample
    assert sample.get('key') == 'value'

    assert sample.set(None, 'value') == sample


def test_map_increment() -> None:
    sample = Map()

    assert 'key' not in sample
    assert sample.increment('key') is sample
    assert sample['key'] == 1
    assert sample.increment('key', 100) is sample
    assert sample.get('key') == 101

    assert sample.increment(None, 'value') == sample


def test_list_append() -> None:
    sample = List()

    assert 'key' not in sample
    assert sample.append('key') is sample
    assert 'key' in sample

    assert sample.append(None) == sample


def test_list_insert() -> None:
    sample = List([1, 2])

    assert sample.insert(0, 0) is sample
    assert sample[0] == 0
    assert len(sample) == 3
    assert sample.insert(0, None) == sample


def test_set_add() -> None:
    sample = Set()

    assert sample.add(0) is sample
    assert 0 in sample
    assert len(sample) == 1
    assert sample.add(None) == sample


# Test the Complex object evaluation


@fixture(scope='module')
def aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_VARIABLE,
        'Name': 'test',
        'Fields': [{
            'Name': 'map_field',
            'Type': Type.MAP,
            'Value': 'test.map_field.set("set", "value").increment("incr", 10).update({"update": "value"})'
        }, {
            'Name': 'list_field',
            'Type': Type.LIST,
            'Value': 'test.list_field.append(1).insert(0, 0).extend([2, 3]).remove(3)'
        }, {
            'Name': 'set_field',
            'Type': Type.SET,
            'Value': 'test.set_field.add(1).copy().union({2, 3}).update({3, 4, 5}).discard(0).remove(1).intersection({2, 4, 5}).symmetric_difference_update({4, 6})'
        }, {
            'Name': 'map_field_cast',
            'Type': Type.MAP,
            'Value': '{"incr": 10} if len(test.map_field_cast) == 0 else test.map_field_cast.increment("incr", 10)'
        }, {
            'Name': 'list_field_cast',
            'Type': Type.LIST,
            'Value': '[0] if len(test.list_field_cast) == 0 else test.list_field_cast.append(1).append(1)'
        }, {
            'Name': 'set_field_cast',
            'Type': Type.SET,
            'Value': 'set({0}) if len(test.set_field_cast) == 0 else test.set_field_cast.add(1).add(2)'
        }]
    }


@fixture(scope='module')
def schema_loader() -> SchemaLoader:
    return SchemaLoader()


@fixture(scope='module')
def aggregate_schema(schema_loader: SchemaLoader,
                     aggregate_schema_spec: Dict[str, Any]) -> AggregateSchema:
    return VariableAggregateSchema(schema_loader.add_schema(aggregate_schema_spec), schema_loader)


@fixture
def aggregate(aggregate_schema: AggregateSchema) -> Aggregate:
    context = EvaluationContext()

    dg = VariableAggregate(schema=aggregate_schema, identity="12345", evaluation_context=context)
    context.global_add('test', dg)
    context.global_add('identity', "12345")

    return dg


def test_field_evaluation(aggregate: Aggregate) -> None:
    assert len(aggregate.map_field) == 0
    assert len(aggregate.set_field) == 0
    assert len(aggregate.list_field) == 0

    aggregate.evaluate()

    assert len(aggregate.map_field) == 3
    assert aggregate.map_field['incr'] == 10
    assert aggregate.map_field['set'] == 'value'
    assert aggregate.map_field['update'] == 'value'

    assert len(aggregate.set_field) == 3
    assert aggregate.set_field == {2, 5, 6}

    assert len(aggregate.list_field) == 3
    assert aggregate.list_field == [0, 1, 2]


def test_field_reset(aggregate: Aggregate) -> None:
    assert len(aggregate.map_field) == 0
    assert len(aggregate.set_field) == 0
    assert len(aggregate.list_field) == 0

    aggregate.evaluate()

    assert len(aggregate.map_field) == 3
    assert len(aggregate.set_field) == 3
    assert len(aggregate.list_field) == 3

    aggregate.reset()

    assert len(aggregate.map_field) == 0
    assert len(aggregate.set_field) == 0
    assert len(aggregate.list_field) == 0


def test_field_multiple_evaluation_type_cast_map(aggregate: Aggregate) -> None:
    assert len(aggregate.map_field_cast) == 0

    aggregate.evaluate()

    assert len(aggregate.map_field_cast) == 1
    assert aggregate.map_field_cast['incr'] == 10

    aggregate.evaluate()

    assert len(aggregate.map_field_cast) == 1
    assert aggregate.map_field_cast['incr'] == 20


def test_field_multiple_evaluation_type_cast_list(aggregate: Aggregate) -> None:
    assert len(aggregate.list_field_cast) == 0

    aggregate.evaluate()

    assert len(aggregate.list_field_cast) == 1
    assert aggregate.list_field_cast == [0]

    aggregate.evaluate()

    assert len(aggregate.list_field_cast) == 3
    assert aggregate.list_field_cast == [0, 1, 1]


def test_field_multiple_evaluation_type_cast_set(aggregate: Aggregate) -> None:
    assert len(aggregate.set_field_cast) == 0

    aggregate.evaluate()

    assert len(aggregate.set_field_cast) == 1
    assert aggregate.set_field_cast == {0}

    aggregate.evaluate()

    assert len(aggregate.set_field_cast) == 3
    assert aggregate.set_field_cast == {0, 1, 2}
