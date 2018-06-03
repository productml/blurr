from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate import AggregateSchema, Aggregate
from blurr.core.evaluation import EvaluationContext
from blurr.core.field import Field
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType
from blurr.core.type import Type

from ciso8601 import parse_datetime


def get_aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_BLOCK,
        'Name': 'user',
        'Store': 'memory',
        'Fields': [{
            'Name': 'event_count',
            'Type': Type.INTEGER,
            'Value': 'user.event_count + 1'
        }]
    }


def get_store_spec() -> Dict[str, Any]:
    return {'Type': Type.BLURR_STORE_MEMORY, 'Name': 'memory'}


class MockAggregateSchema(AggregateSchema):
    pass


class MockAggregate(Aggregate):
    pass


@fixture
def aggregate_schema_with_store():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(get_aggregate_schema_spec())
    schema_loader.add_schema_spec(get_store_spec(), 'user')
    return MockAggregateSchema(fully_qualified_name=name, schema_loader=schema_loader)


@fixture
def aggregate_schema_without_store():
    schema_loader = SchemaLoader()
    aggregate_schema_spec = get_aggregate_schema_spec()
    del aggregate_schema_spec['Store']
    name = schema_loader.add_schema_spec(aggregate_schema_spec)
    return MockAggregateSchema(fully_qualified_name=name, schema_loader=schema_loader)


def test_aggregate_initialization(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity='12345',
        evaluation_context=EvaluationContext())
    assert aggregate._identity == '12345'


def test_aggregate_nested_items(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity='12345',
        evaluation_context=EvaluationContext())
    nested_items = aggregate._nested_items
    assert len(nested_items) == 3
    assert 'event_count' in nested_items
    assert isinstance(nested_items['event_count'], Field)
    assert '_identity' in nested_items
    assert isinstance(nested_items['_identity'], Field)
    assert '_processed_tracker' in nested_items
    assert isinstance(nested_items['_processed_tracker'], Field)


def test_aggregate_persist_without_store(aggregate_schema_without_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_without_store,
        identity='12345',
        evaluation_context=EvaluationContext())
    aggregate._persist()


def test_aggregate_persist_with_store(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity='12345',
        evaluation_context=EvaluationContext())
    aggregate._persist()
    snapshot_aggregate = aggregate._store.get(
        Key(KeyType.DIMENSION, identity='12345', group='user'))
    assert snapshot_aggregate is not None
    assert snapshot_aggregate == aggregate._snapshot


def test_aggregate_finalize(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity='12345',
        evaluation_context=EvaluationContext())
    aggregate.run_finalize()
    snapshot_aggregate = aggregate._store.get(
        Key(KeyType.DIMENSION, identity='12345', group='user'))
    assert snapshot_aggregate is not None
    assert snapshot_aggregate == aggregate._snapshot


def test_aggregate_exactly_once_execution_per_record(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity='12345',
        evaluation_context=EvaluationContext())

    record = Record({
        'id': 'user1',
        'event_value': 10000,
        'event_time': '2018-01-02T01:01:01+00:00'
    })

    aggregate._evaluation_context.global_add('source', record)
    aggregate._evaluation_context.global_add('identity', record.id)
    aggregate._evaluation_context.global_add('time', parse_datetime(record.event_time))
    aggregate._evaluation_context.global_add('user', aggregate)
    aggregate.run_evaluate()

    assert aggregate.event_count == 1
    assert parse_datetime(record.event_time).isoformat() in aggregate._processed_tracker

    aggregate.run_evaluate()

    assert aggregate.event_count == 1

    record = Record({
        'id': 'user1',
        'event_value': 10000,
        'event_time': '2018-01-02T01:01:02+00:00'
    })

    aggregate._evaluation_context.global_add('source', record)
    aggregate._evaluation_context.global_add('time', parse_datetime(record.event_time))

    aggregate.run_evaluate()
    aggregate.run_evaluate()

    assert aggregate.event_count == 2
