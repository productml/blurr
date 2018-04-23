from datetime import datetime, timezone
from typing import Dict, Any, List

from dateutil import parser
from pytest import fixture

from blurr.core.aggregate_label import LabelAggregateSchema, LabelAggregate
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.store.memory_store import MemoryStore


@fixture
def label_aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': 'Blurr:Aggregate:LabelAggregate',
        'Name': 'label_aggr',
        'Store': 'memory',
        'LabelFields': [{
            'Name': 'label',
            'Type': 'string',
            'Value': 'source.label'
        }],
        'Fields': [{
            'Name': 'sum',
            'Type': 'integer',
            'Value': 'label_aggr.sum + source.event_value'
        }, {
            'Name': 'count',
            'Type': 'integer',
            'Value': 'label_aggr.count + 1'
        }]
    }


@fixture
def label_aggregate_schema_spec_with_two_label_fields() -> Dict[str, Any]:
    return {
        'Type': 'Blurr:Aggregate:LabelAggregate',
        'Name': 'label_aggr',
        'Store': 'memory',
        'LabelFields': [{
            'Name': 'label',
            'Type': 'string',
            'Value': 'source.label'
        }, {
            'Name': 'label_ascii',
            'Type': 'integer',
            'Value': 'ord(source.label)'
        }],
        'Fields': [{
            'Name': 'sum',
            'Type': 'integer',
            'Value': 'label_aggr.sum + source.event_value'
        }, {
            'Name': 'count',
            'Type': 'integer',
            'Value': 'label_aggr.count + 1'
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Type': 'Blurr:Store:MemoryStore', 'Name': 'memory'}


@fixture
def label_events() -> List[Record]:
    return [
        Record({
            'id': 'user1',
            'label': 'a',
            'event_value': 10,
            'event_time': '2018-01-01T01:01:01+00:00'
        }),
        Record({
            'id': 'user1',
            'label': 'b',
            'event_value': 1,
            'event_time': '2018-01-01T01:02:01+00:00'
        }),
        Record({
            'id': 'user1',
            'label': 'a',
            'event_value': 100,
            'event_time': '2018-01-01T01:01:05+00:00'
        }),
        Record({
            'id': 'user1',
            'label': 'c',
            'event_value': 10000,
            'event_time': '2018-01-02T01:01:01+00:00'
        }),
        Record({
            'id': 'user1',
            'label': 'c',
            'event_value': 1000,
            'event_time': '2018-01-01T03:01:01+00:00'
        }),
    ]


def label_aggregate_schema(label_aggregate_schema_spec: Dict[str, Any],
                           store_spec: Dict[str, Any]) -> LabelAggregateSchema:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(label_aggregate_schema_spec)
    schema_loader.add_schema(store_spec, name)
    return LabelAggregateSchema(name, schema_loader)


def test_label_aggregate_schema_initialization(label_aggregate_schema_spec: Dict[str, Any],
                                               store_spec: Dict[str, Any]):
    schema = label_aggregate_schema(label_aggregate_schema_spec, store_spec)
    assert isinstance(schema.store, MemoryStore)
    assert isinstance(schema.label_fields, dict)


def evaluate_event(record: Record, aggregate: LabelAggregate) -> None:
    aggregate._evaluation_context.global_add('source', record)
    aggregate._evaluation_context.global_add('time', parser.parse(record.event_time))
    aggregate.evaluate()


def test_split_by_label_valid(label_aggregate_schema_spec: Dict[str, Any],
                              store_spec: Dict[str, Any], label_events: List[Record]):
    schema = label_aggregate_schema(label_aggregate_schema_spec, store_spec)
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    label_aggregate = LabelAggregate(schema, identity, evaluation_context)
    evaluation_context.global_add(label_aggregate._schema.name, label_aggregate)

    # Check that initial state is empty
    assert label_aggregate._label_fields['label'].value == ''
    assert label_aggregate._schema.store.get_all() == {}

    # Check state at the end of the first event processed
    evaluate_event(label_events[0], label_aggregate)

    assert label_aggregate._label_fields['label'].value == 'a'
    assert label_aggregate._schema.store.get_all() == {}

    # Check for labeled partition and persistence of the first label when label changes
    evaluate_event(label_events[1], label_aggregate)
    assert label_aggregate._label_fields['label'].value == 'b'

    evaluate_event(label_events[2], label_aggregate)
    label_aggregate.persist()
    store_state = label_aggregate._schema.store.get_all()

    assert label_aggregate._label_fields['label'].value == 'a'
    assert len(store_state) == 2

    # Check for final state
    evaluate_event(label_events[3], label_aggregate)
    evaluate_event(label_events[4], label_aggregate)
    label_aggregate.persist()
    assert label_aggregate._label_fields['label'].value == 'c'
    store_state = label_aggregate._schema.store.get_all()
    assert len(store_state) == 3

    assert store_state.get(Key('user1', 'label_aggr.a')) == {
        '_identity': 'user1',
        'label': 'a',
        '_start_time': datetime(2018, 1, 1, 1, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 1, 1, 1, 5, 0, timezone.utc),
        'sum': 110,
        'count': 2
    }

    assert store_state.get(Key('user1', 'label_aggr.b')) == {
        '_identity': 'user1',
        'label': 'b',
        '_start_time': datetime(2018, 1, 1, 1, 2, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 1, 1, 2, 1, 0, timezone.utc),
        'sum': 1,
        'count': 1
    }

    assert store_state.get(Key('user1', 'label_aggr.c')) == {
        '_identity': 'user1',
        'label': 'c',
        '_start_time': datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc),
        'sum': 11000,
        'count': 2
    }


def test_split_when_label_evaluates_to_none(label_aggregate_schema_spec: Dict[str, Any],
                                            store_spec: Dict[str, Any], label_events: List[Record]):
    label_aggregate_schema_spec['LabelFields'][0][
        'Value'] = '1/0 if source.label == \'a\' else source.label'
    schema = label_aggregate_schema(label_aggregate_schema_spec, store_spec)
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    label_aggregate = LabelAggregate(schema, identity, evaluation_context)
    evaluation_context.global_add(label_aggregate._schema.name, label_aggregate)

    # Check for error states
    evaluate_event(label_events[0], label_aggregate)
    evaluate_event(label_events[1], label_aggregate)
    evaluate_event(label_events[2], label_aggregate)
    assert label_aggregate._label_fields['label'].value == 'b'

    label_aggregate.finalize()

    store_state = label_aggregate._schema.store.get_all()
    assert len(store_state) == 1

    assert store_state.get(Key('user1', 'label_aggr.b')) == {
        '_identity': 'user1',
        'label': 'b',
        '_start_time': datetime(2018, 1, 1, 1, 2, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 1, 1, 2, 1, 0, timezone.utc),
        'sum': 1,
        'count': 1
    }


def test_two_label_fields_in_aggregate(
        label_aggregate_schema_spec_with_two_label_fields: Dict[str, Any],
        store_spec: Dict[str, Any], label_events: List[Record]):
    schema = label_aggregate_schema(label_aggregate_schema_spec_with_two_label_fields, store_spec)
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    label_aggregate = LabelAggregate(schema, identity, evaluation_context)
    evaluation_context.global_add(label_aggregate._schema.name, label_aggregate)

    # Evaluate all the events
    for event in label_events:
        evaluate_event(event, label_aggregate)

    label_aggregate.finalize()

    store_state = label_aggregate._schema.store.get_all()
    assert len(store_state) == 3

    assert store_state.get(Key('user1', 'label_aggr.a:97')) == {
        '_identity': 'user1',
        'label': 'a',
        'label_ascii': 97,
        '_start_time': datetime(2018, 1, 1, 1, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 1, 1, 1, 5, 0, timezone.utc),
        'sum': 110,
        'count': 2
    }

    assert store_state.get(Key('user1', 'label_aggr.b:98')) == {
        '_identity': 'user1',
        'label': 'b',
        'label_ascii': 98,
        '_start_time': datetime(2018, 1, 1, 1, 2, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 1, 1, 2, 1, 0, timezone.utc),
        'sum': 1,
        'count': 1
    }

    assert store_state.get(Key('user1', 'label_aggr.c:99')) == {
        '_identity': 'user1',
        'label': 'c',
        'label_ascii': 99,
        '_start_time': datetime(2018, 1, 1, 3, 1, 1, 0, timezone.utc),
        '_end_time': datetime(2018, 1, 2, 1, 1, 1, 0, timezone.utc),
        'sum': 11000,
        'count': 2
    }
