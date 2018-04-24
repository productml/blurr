from datetime import datetime, timezone
from typing import Dict, Any, List

from dateutil import parser
from pytest import fixture

from blurr.core.aggregate_identity import IdentityAggregateSchema, IdentityAggregate
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.type import Type
from blurr.store.memory_store import MemoryStore


@fixture
def identity_aggregate_schema_spec() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_IDENTITY,
        'Name': 'label_aggr',
        'Store': 'memory',
        'Dimensions': [{
            'Name': 'label',
            'Type': Type.STRING,
            'Value': 'source.label'
        }],
        'Fields': [{
            'Name': 'sum',
            'Type': Type.INTEGER,
            'Value': 'label_aggr.sum + source.event_value'
        }, {
            'Name': 'count',
            'Type': Type.INTEGER,
            'Value': 'label_aggr.count + 1'
        }]
    }


@fixture
def identity_aggregate_schema_spec_with_two_key_fields() -> Dict[str, Any]:
    return {
        'Type': Type.BLURR_AGGREGATE_IDENTITY,
        'Name': 'label_aggr',
        'Store': 'memory',
        'Dimensions': [{
            'Name': 'label',
            'Type': Type.STRING,
            'Value': 'source.label'
        }, {
            'Name': 'label_ascii',
            'Type': Type.INTEGER,
            'Value': 'ord(source.label)'
        }],
        'Fields': [{
            'Name': 'sum',
            'Type': Type.INTEGER,
            'Value': 'label_aggr.sum + source.event_value'
        }, {
            'Name': 'count',
            'Type': Type.INTEGER,
            'Value': 'label_aggr.count + 1'
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Type': Type.BLURR_STORE_MEMORY, 'Name': 'memory'}


@fixture
def records() -> List[Record]:
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


def identity_aggregate_schema(identity_aggregate_schema_spec: Dict[str, Any],
                              store_spec: Dict[str, Any]) -> IdentityAggregateSchema:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(identity_aggregate_schema_spec)
    schema_loader.add_schema(store_spec, name)
    return IdentityAggregateSchema(name, schema_loader)


def test_schema_initialization(identity_aggregate_schema_spec: Dict[str, Any],
                               store_spec: Dict[str, Any]):
    schema = identity_aggregate_schema(identity_aggregate_schema_spec, store_spec)
    assert isinstance(schema.store, MemoryStore)
    assert isinstance(schema.dimension_fields, dict)


def evaluate_event(record: Record, aggregate: IdentityAggregate) -> None:
    aggregate._evaluation_context.global_add('source', record)
    aggregate._evaluation_context.global_add('time', parser.parse(record.event_time))
    aggregate.evaluate()


def test_split_by_label_valid(identity_aggregate_schema_spec: Dict[str, Any],
                              store_spec: Dict[str, Any], records: List[Record]):
    schema = identity_aggregate_schema(identity_aggregate_schema_spec, store_spec)
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    identity_aggregate = IdentityAggregate(schema, identity, evaluation_context)
    evaluation_context.global_add(identity_aggregate._schema.name, identity_aggregate)

    # Check that initial state is empty
    assert identity_aggregate._dimension_fields['label'].value == ''
    assert identity_aggregate._schema.store.get_all() == {}

    # Check state at the end of the first event processed
    evaluate_event(records[0], identity_aggregate)

    assert identity_aggregate._dimension_fields['label'].value == 'a'
    assert identity_aggregate._schema.store.get_all() == {}

    # Check for labeled partition and persistence of the first label when label changes
    evaluate_event(records[1], identity_aggregate)
    assert identity_aggregate._dimension_fields['label'].value == 'b'

    evaluate_event(records[2], identity_aggregate)
    identity_aggregate.persist()
    store_state = identity_aggregate._schema.store.get_all()

    assert identity_aggregate._dimension_fields['label'].value == 'a'
    assert len(store_state) == 2

    # Check for final state
    evaluate_event(records[3], identity_aggregate)
    evaluate_event(records[4], identity_aggregate)
    identity_aggregate.persist()
    assert identity_aggregate._dimension_fields['label'].value == 'c'
    store_state = identity_aggregate._schema.store.get_all()
    assert len(store_state) == 3

    assert store_state.get(Key('user1', 'label_aggr.a')) == {
        '_identity': 'user1',
        'label': 'a',
        'sum': 110,
        'count': 2
    }

    assert store_state.get(Key('user1', 'label_aggr.b')) == {
        '_identity': 'user1',
        'label': 'b',
        'sum': 1,
        'count': 1
    }

    assert store_state.get(Key('user1', 'label_aggr.c')) == {
        '_identity': 'user1',
        'label': 'c',
        'sum': 11000,
        'count': 2
    }


def test_split_when_label_evaluates_to_none(identity_aggregate_schema_spec: Dict[str, Any],
                                            store_spec: Dict[str, Any], records: List[Record]):
    identity_aggregate_schema_spec['Dimensions'][0][
        'Value'] = '1/0 if source.label == \'a\' else source.label'
    schema = identity_aggregate_schema(identity_aggregate_schema_spec, store_spec)
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    identity_aggregate = IdentityAggregate(schema, identity, evaluation_context)
    evaluation_context.global_add(identity_aggregate._schema.name, identity_aggregate)

    # Check for error states
    evaluate_event(records[0], identity_aggregate)
    evaluate_event(records[1], identity_aggregate)
    evaluate_event(records[2], identity_aggregate)
    assert identity_aggregate._dimension_fields['label'].value == 'b'

    identity_aggregate.finalize()

    store_state = identity_aggregate._schema.store.get_all()
    assert len(store_state) == 1

    assert store_state.get(Key('user1', 'label_aggr.b')) == {
        '_identity': 'user1',
        'label': 'b',
        'sum': 1,
        'count': 1
    }


def test_two_key_fields_in_aggregate(
        identity_aggregate_schema_spec_with_two_key_fields: Dict[str, Any],
        store_spec: Dict[str, Any], records: List[Record]):
    schema = identity_aggregate_schema(identity_aggregate_schema_spec_with_two_key_fields,
                                       store_spec)
    # Initialize the starting state
    identity = 'user1'
    evaluation_context = EvaluationContext()
    evaluation_context.global_add('identity', identity)
    identity_aggregate = IdentityAggregate(schema, identity, evaluation_context)
    evaluation_context.global_add(identity_aggregate._schema.name, identity_aggregate)

    # Evaluate all the events
    for event in records:
        evaluate_event(event, identity_aggregate)

    identity_aggregate.finalize()

    store_state = identity_aggregate._schema.store.get_all()
    assert len(store_state) == 3

    assert store_state.get(Key('user1', 'label_aggr.a:97')) == {
        '_identity': 'user1',
        'label': 'a',
        'label_ascii': 97,
        'sum': 110,
        'count': 2
    }

    assert store_state.get(Key('user1', 'label_aggr.b:98')) == {
        '_identity': 'user1',
        'label': 'b',
        'label_ascii': 98,
        'sum': 1,
        'count': 1
    }

    assert store_state.get(Key('user1', 'label_aggr.c:99')) == {
        '_identity': 'user1',
        'label': 'c',
        'label_ascii': 99,
        'sum': 11000,
        'count': 2
    }
