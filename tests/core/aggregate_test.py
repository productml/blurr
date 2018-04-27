from datetime import datetime, timezone
from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate import AggregateSchema, Aggregate
from blurr.core.type import Type
from blurr.core.evaluation import EvaluationContext
from blurr.core.field import Field
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


def get_aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_BLOCK,
        'Name': 'user',
        'Store': 'memory',
        'Fields': [{
            'Name': 'event_count',
            'Type': Type.INTEGER,
            'Value': 5
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
    name = schema_loader.add_schema(get_aggregate_schema_spec())
    schema_loader.add_schema(get_store_spec(), 'user')
    return MockAggregateSchema(fully_qualified_name=name, schema_loader=schema_loader)


@fixture
def aggregate_schema_without_store():
    schema_loader = SchemaLoader()
    aggregate_schema_spec = get_aggregate_schema_spec()
    del aggregate_schema_spec['Store']
    name = schema_loader.add_schema(aggregate_schema_spec)
    return MockAggregateSchema(fully_qualified_name=name, schema_loader=schema_loader)


def test_aggregate_initialization(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    assert aggregate._identity == "12345"


def test_aggregate_nested_items(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    nested_items = aggregate._nested_items
    assert len(nested_items) == 2
    assert "event_count" in nested_items
    assert isinstance(nested_items["event_count"], Field)
    assert "_identity" in nested_items
    assert isinstance(nested_items["_identity"], Field)


def test_aggregate_persist_without_store(aggregate_schema_without_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_without_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    aggregate.persist()


def test_aggregate_persist_with_store(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    dt = datetime.now()
    dt.replace(tzinfo=timezone.utc)
    aggregate.persist(dt)
    snapshot_aggregate = aggregate._store.get(
        Key(identity="12345", group="user", timestamp=dt))
    assert snapshot_aggregate is not None
    assert snapshot_aggregate == aggregate._snapshot


def test_aggregate_finalize(aggregate_schema_with_store):
    aggregate = MockAggregate(
        schema=aggregate_schema_with_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    aggregate.finalize()
    snapshot_aggregate = aggregate._store.get(Key(identity="12345", group="user"))
    assert snapshot_aggregate is not None
    assert snapshot_aggregate == aggregate._snapshot
