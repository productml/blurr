from datetime import datetime, timezone
from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate_block import BlockAggregateSchema, \
    BlockAggregate
from blurr.core.evaluation import EvaluationContext
from blurr.core.field import Field
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType
from blurr.core.type import Type


@fixture
def block_aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_BLOCK,
        'Name': 'user',
        'Store': 'memstore',
        'Fields': [{
            'Name': 'event_count',
            'Type': Type.INTEGER,
            'Value': 'user.event_count + 1'
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Name': 'memstore', 'Type': Type.BLURR_STORE_MEMORY}


@fixture
def schema_loader(store_spec) -> SchemaLoader:
    schema_loader = SchemaLoader()
    schema_loader.add_schema_spec(store_spec, 'user')
    return schema_loader


def check_fields(fields: Dict[str, Field], expected_field_values: Dict[str, Any]) -> bool:
    if len(fields) != len(expected_field_values):
        return False

    for field_name, field in fields.items():
        if not isinstance(field, Field):
            return False
        if field.value != expected_field_values[field_name]:
            return False

    return True


def create_block_aggregate(schema, time, identity) -> BlockAggregate:
    evaluation_context = EvaluationContext()
    block_aggregate = BlockAggregate(
        schema=schema, identity=identity, evaluation_context=evaluation_context)
    evaluation_context.global_add('time', time)
    evaluation_context.global_add('user', block_aggregate)
    evaluation_context.global_add('identity', identity)
    return block_aggregate


def test_block_aggregate_schema_evaluate_without_split(block_aggregate_schema_spec, schema_loader):
    name = schema_loader.add_schema_spec(block_aggregate_schema_spec)
    block_aggregate_schema = BlockAggregateSchema(name, schema_loader)

    identity = 'userA'
    time = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    block_aggregate = create_block_aggregate(block_aggregate_schema, time, identity)
    block_aggregate.run_evaluate()

    # Check eval results of various fields
    assert len(block_aggregate._nested_items) == 4
    assert check_fields(block_aggregate._nested_items, {
        '_identity': identity,
        'event_count': 1,
        '_start_time': time,
        '_end_time': time
    })

    # aggregate snapshot should not exist in store
    assert block_aggregate._store.get(
        Key(key_type=KeyType.DIMENSION,
            identity=block_aggregate._identity,
            group=block_aggregate._name)) is None


def test_block_aggregate_schema_evaluate_with_dimensions(block_aggregate_schema_spec,
                                                         schema_loader):
    block_aggregate_schema_spec['Dimensions'] = [{
        'Name': 'label',
        'Type': Type.STRING,
        'Value': 'source.label'
    }]
    name = schema_loader.add_schema_spec(block_aggregate_schema_spec)
    block_aggregate_schema = BlockAggregateSchema(name, schema_loader)

    identity = 'userA'
    time = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    block_aggregate = create_block_aggregate(block_aggregate_schema, time, identity)
    block_aggregate._evaluation_context.global_add('source', Record({'label': 'label1'}))
    block_aggregate.run_evaluate()
    block_aggregate.run_evaluate()

    # Check eval results of various fields before split
    assert check_fields(
        block_aggregate._nested_items, {
            '_identity': identity,
            'event_count': 2,
            '_start_time': time,
            '_end_time': time,
            'label': 'label1',
        })

    current_snapshot = block_aggregate._snapshot
    block_aggregate._evaluation_context.global_add('source', Record({'label': 'label2'}))
    block_aggregate.run_evaluate()

    # Check eval results of various fields
    assert check_fields(
        block_aggregate._nested_items, {
            '_identity': identity,
            'event_count': 1,
            '_start_time': time,
            '_end_time': time,
            'label': 'label2',
        })

    # Check aggregate snapshot present in store
    assert block_aggregate._store.get(
        Key(key_type=KeyType.DIMENSION,
            identity=block_aggregate._identity,
            group=block_aggregate._name,
            dimensions=['label1'])) == current_snapshot
