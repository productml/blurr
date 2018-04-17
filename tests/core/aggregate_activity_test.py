from datetime import datetime, timezone
from typing import Dict, Any

from dateutil import parser
from pytest import fixture

from blurr.core.aggregate_activity import ActivityAggregate, ActivityAggregateSchema
from blurr.core.evaluation import EvaluationContext
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


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
def activity_events():
    return [
        Record({'id': 'user1', 'event_value': 10, 'event_time': '2018-01-01T01:01:01+00:00'}),
        Record({'id': 'user1', 'event_value': 100, 'event_time': '2018-01-01T01:01:05+00:00'}),
        Record({'id': 'user1', 'event_value': 1, 'event_time': '2018-01-01T01:02:01+00:00'}),
        Record({'id': 'user1', 'event_value': 1000, 'event_time': '2018-01-01T03:01:01+00:00'}),
        Record({'id': 'user1', 'event_value': 10000, 'event_time': '2018-01-02T01:01:01+00:00'})
    ]


def test_split_by_inactivity(activity_aggregate_schema, activity_events):
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    activity_aggregate = ActivityAggregate(activity_aggregate_schema, identity, evaluation_context)
    evaluation_context.global_add(activity_aggregate._schema.name, activity_aggregate)

    for record in activity_events:
        evaluation_context.global_add('source', record)
        evaluation_context.global_add('time', parser.parse(record.event_time))
        activity_aggregate.evaluate()

    activity_aggregate.finalize()

    store_state = activity_aggregate._schema.store.get_all()
    assert len(store_state) == 3
    assert store_state.get(Key('user1', 'activity_aggr', datetime(2018, 1, 1, 1, 1, 1, 0, timezone.utc))) == {
        '_identity': 'user1', '_start_time': datetime(2018, 1, 1, 1, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 1, 1, 2, 1, 0, timezone.utc), 'sum': 111, 'count': 3}

    assert store_state.get(Key('user1', 'activity_aggr', datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc))) == {
        '_identity': 'user1', '_start_time': datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc), 'sum': 1000, 'count': 1}

    assert store_state.get(Key('user1', 'activity_aggr', datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc))) == {
        '_identity': 'user1', '_start_time': datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc), 'sum': 10000, 'count': 1}
