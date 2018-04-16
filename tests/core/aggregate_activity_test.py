from datetime import datetime, timezone
from dateutil import parser
from typing import Dict, Any

from pytest import fixture

from blurr.core.aggregate_activity import ActivityAggregate, ActivityAggregateSchema
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.store.memory_store import MemoryStore


@fixture
def activity_aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': 'Blurr:Aggregate:ActivityAggregate',
        'Name': 'activity_aggr',
        'SeparateByInactiveSeconds': '1800',
        'Store': 'memory',
        'Fields': [{
            'Name': 'sum',
            'Type': 'integer',
            'Value': 'activity_aggr.sum + source.event_value'
        }, {
            'Name': 'count',
            'Type': 'integer',
            'Value': 'activity_aggr.count + 1'
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Type': 'Blurr:Store:MemoryStore', 'Name': 'memory'}


@fixture
def activity_aggregate_schema(activity_aggregate_schema_spec, store_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(activity_aggregate_schema_spec)
    schema_loader.add_schema(store_spec, name)
    return ActivityAggregateSchema(name, schema_loader)


@fixture
def label_events():
    return [
        Record({'id': 'user1', 'label': 'a', 'event_value': 10, 'event_time': '2018-01-01T01:01:01+00:00'}),
        Record({'id': 'user1', 'label': 'a', 'event_value': 100, 'event_time': '2018-01-01T01:01:05+00:00'}),
        Record({'id': 'user1', 'label': 'b', 'event_value': 1, 'event_time': '2018-01-01T01:02:01+00:00'}),
        Record({'id': 'user1', 'label': 'c', 'event_value': 1000, 'event_time': '2018-01-01T03:01:01+00:00'}),
        Record({'id': 'user1', 'label': 'c', 'event_value': 10000, 'event_time': '2018-01-02T01:01:01+00:00'})
    ]


def test_label_aggregate_schema_initialization(label_aggregate_schema):
    assert isinstance(label_aggregate_schema.store, MemoryStore)
    assert isinstance(label_aggregate_schema.label, Expression)


def test_split_by_label(label_aggregate_schema, label_events):

    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    label_aggregate = LabelAggregate(label_aggregate_schema, identity, evaluation_context)
    evaluation_context.global_add(label_aggregate._schema.name, label_aggregate)

    # Check that initial state is empty
    assert label_aggregate._label is None
    assert label_aggregate._schema.store.get_all() == {}

    def evaluate_event(index):
        record = label_events[index]
        evaluation_context.global_add('source', record)
        evaluation_context.global_add('time', parser.parse(record.event_time))
        label_aggregate.evaluate()

    # Check state at the end of the first event processed
    evaluate_event(0)
    assert label_aggregate._label == 'a'
    assert label_aggregate._schema.store.get_all() == {}

    # Check for labeled partition and persistence of the first label when label changes
    evaluate_event(1)
    evaluate_event(2)
    assert label_aggregate._label == 'b'
    store_state = label_aggregate._schema.store.get_all()
    assert len(store_state) == 1
    assert store_state.get(Key('user1', 'label_aggr.a', datetime(2018, 1, 1, 1, 1, 1, 0, timezone.utc))) == {
        '_identity': 'user1', '_start_time': datetime(2018, 1, 1, 1, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 1, 1, 1, 5, 0, timezone.utc), 'sum': 110, 'count': 2}

    # Check for final state
    evaluate_event(3)
    evaluate_event(4)
    label_aggregate.finalize()
    assert label_aggregate._label == 'c'
    store_state = label_aggregate._schema.store.get_all()
    assert len(store_state) == 3
    assert store_state.get(Key('user1', 'label_aggr.c', datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc))) == {
        '_identity': 'user1', '_start_time': datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc), 'sum': 11000, 'count': 2}




