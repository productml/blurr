from datetime import datetime, timezone
from typing import Dict, Any, List

from dateutil import parser
from pytest import fixture

from blurr.core.aggregate_activity import ActivityAggregate, ActivityAggregateSchema
from blurr.core.errors import RequiredAttributeError, InvalidNumberError
from blurr.core.evaluation import EvaluationContext
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType
from blurr.core.type import Type
from tests.core.conftest import assert_aggregate_snapshot_equals


@fixture
def activity_aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_ACTIVITY,
        'Name': 'activity_aggr',
        'SeparateByInactiveSeconds': '1800',
        'Store': 'memory',
        'Fields': [{
            'Name': 'sum',
            'Type': Type.INTEGER,
            'Value': 'activity_aggr.sum + source.event_value'
        }, {
            'Name': 'count',
            'Type': Type.INTEGER,
            'Value': 'activity_aggr.count + 1'
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Type': Type.BLURR_STORE_MEMORY, 'Name': 'memory'}


@fixture
def activity_aggregate_schema(activity_aggregate_schema_spec: Dict[str, Any],
                              store_spec: Dict[str, Any]) -> ActivityAggregateSchema:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(activity_aggregate_schema_spec)
    schema_loader.add_schema_spec(store_spec, name)
    return ActivityAggregateSchema(name, schema_loader)


@fixture
def activity_events() -> List[Record]:
    return [
        Record({
            'id': 'user1',
            'event_value': 10,
            'event_time': '2018-01-01T01:01:01+00:00'
        }),
        Record({
            'id': 'user1',
            'event_value': 100,
            'event_time': '2018-01-01T01:01:05+00:00'
        }),
        Record({
            'id': 'user1',
            'event_value': 1,
            'event_time': '2018-01-01T01:02:01+00:00'
        }),
        Record({
            'id': 'user1',
            'event_value': 1000,
            'event_time': '2018-01-01T03:01:01+00:00'
        }),
        Record({
            'id': 'user1',
            'event_value': 10000,
            'event_time': '2018-01-02T01:01:01+00:00'
        })
    ]


def evaluate_event(record: Record, aggregate: ActivityAggregate) -> None:
    aggregate._evaluation_context.global_add('source', record)
    aggregate._evaluation_context.global_add('time', parser.parse(record.event_time))
    aggregate.run_evaluate()


def test_aggregate_final_state(activity_aggregate_schema: ActivityAggregateSchema,
                               activity_events: List[Record]) -> None:
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    activity_aggregate = ActivityAggregate(activity_aggregate_schema, identity, evaluation_context)
    evaluation_context.global_add(activity_aggregate._schema.name, activity_aggregate)

    for record in activity_events:
        evaluate_event(record, activity_aggregate)

    activity_aggregate.run_finalize()

    store_state = activity_aggregate._store.get_all(identity)
    assert len(store_state) == 3
    assert_aggregate_snapshot_equals(store_state.get(
        Key(KeyType.TIMESTAMP, 'user1', 'activity_aggr', [],
            datetime(2018, 1, 1, 1, 1, 1, 0, timezone.utc))), {
                '_start_time': datetime(2018, 1, 1, 1, 1, 1, 0, timezone.utc).isoformat(),
                '_end_time': datetime(2018, 1, 1, 1, 2, 1, 0, timezone.utc).isoformat(),
                'sum': 111,
                'count': 3
            })

    assert_aggregate_snapshot_equals(store_state.get(
        Key(KeyType.TIMESTAMP, 'user1', 'activity_aggr', [],
            datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc))), {
                '_start_time': datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc).isoformat(),
                '_end_time': datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc).isoformat(),
                'sum': 1000,
                'count': 1
            })

    assert_aggregate_snapshot_equals(store_state.get(
        Key(KeyType.TIMESTAMP, 'user1', 'activity_aggr', [],
            datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc))), {
                '_start_time': datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc).isoformat(),
                '_end_time': datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc).isoformat(),
                'sum': 10000,
                'count': 1
            })


def test_evaluate_no_separation(activity_aggregate_schema: ActivityAggregateSchema,
                                activity_events: List[Record]) -> None:
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    activity_aggregate = ActivityAggregate(activity_aggregate_schema, identity, evaluation_context)
    evaluation_context.global_add(activity_aggregate._schema.name, activity_aggregate)

    store_state = activity_aggregate._store.get_all(identity)
    assert len(store_state) == 0

    evaluate_event(activity_events[0], activity_aggregate)
    activity_aggregate._persist()

    store_state = activity_aggregate._store.get_all(identity)
    assert len(store_state) == 1

    evaluate_event(activity_events[1], activity_aggregate)
    activity_aggregate._persist()

    store_state = activity_aggregate._store.get_all(identity)
    assert len(store_state) == 1


def test_evaluate_separate_on_inactivity(activity_aggregate_schema: ActivityAggregateSchema,
                                         activity_events: List[Record]) -> None:
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    activity_aggregate = ActivityAggregate(activity_aggregate_schema, identity, evaluation_context)
    evaluation_context.global_add(activity_aggregate._schema.name, activity_aggregate)

    evaluate_event(activity_events[2], activity_aggregate)
    activity_aggregate._persist()

    store_state = activity_aggregate._store.get_all(identity)
    assert len(store_state) == 1

    evaluate_event(activity_events[3], activity_aggregate)
    activity_aggregate._persist()

    store_state = activity_aggregate._store.get_all(identity)
    assert len(store_state) == 2


def get_schema(schema_spec, store_spec):
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema_spec(schema_spec)
    schema_loader.add_schema_spec(store_spec, name)
    return ActivityAggregateSchema(name, schema_loader)


def test_activity_aggregate_schema_missing_separate_by_inactive_attribute_adds_error(
        activity_aggregate_schema_spec, store_spec):
    del activity_aggregate_schema_spec[
        ActivityAggregateSchema.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS]
    schema = get_schema(activity_aggregate_schema_spec, store_spec)

    assert 1 == len(schema.errors)
    assert isinstance(schema.errors[0], RequiredAttributeError)
    assert ActivityAggregateSchema.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS == schema.errors[
        0].attribute


def test_activity_aggregate_schema_non_integer_separative_by_inactive_attribute_adds_error(
        activity_aggregate_schema_spec, store_spec):
    activity_aggregate_schema_spec[
        ActivityAggregateSchema.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS] = 'non-integer'
    schema = get_schema(activity_aggregate_schema_spec, store_spec)

    assert isinstance(schema.errors[0], InvalidNumberError)

    activity_aggregate_schema_spec[
        ActivityAggregateSchema.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS] = -1
    schema = get_schema(activity_aggregate_schema_spec, store_spec)

    assert int == schema.errors[0].type
    assert 1 == schema.errors[0].min
