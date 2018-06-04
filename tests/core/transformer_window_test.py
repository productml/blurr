from datetime import datetime, timezone

import pytest
import yaml
from pytest import fixture

from blurr.core.aggregate_block import BlockAggregate, TimeAggregate
from blurr.core.anchor import AnchorSchema
from blurr.core.errors import PrepareWindowMissingBlocksError, RequiredAttributeError
from blurr.core.evaluation import Context
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType
from blurr.core.transformer_streaming import StreamingTransformer
from blurr.core.transformer_window import WindowTransformer, WindowTransformerSchema
from blurr.core.type import Type
from tests.core.conftest import init_memory_store, assert_aggregate_snapshot_equals


@fixture
def stream_schema_spec():
    return yaml.safe_load(open('tests/data/stream.yml'))


@fixture
def window_schema_spec():
    return yaml.safe_load(open('tests/data/window.yml'))


@fixture
def schema_loader():
    return SchemaLoader()


@fixture
def stream_transformer(schema_loader, stream_schema_spec):
    stream_bts_name = schema_loader.add_schema_spec(stream_schema_spec)
    stream_transformer = StreamingTransformer(
        schema_loader.get_schema_object(stream_bts_name), 'user1')
    stream_transformer.run_restore({Key(KeyType.DIMENSION, 'user1', 'state'): {'country': 'US'}})
    return stream_transformer


@fixture
def window_transformer(schema_loader, stream_transformer, window_schema_spec):
    window_bts_name = schema_loader.add_schema_spec(window_schema_spec)
    return WindowTransformer(
        schema_loader.get_schema_object(window_bts_name), 'user1',
        Context({
            stream_transformer._schema.name: stream_transformer
        }))


@fixture
def time_aggregate(stream_transformer):
    block = None
    for aggregate in stream_transformer._nested_items.values():
        if isinstance(aggregate, TimeAggregate):
            block = aggregate

    return block


def test_window_transformer_schema_init(schema_loader, stream_schema_spec, window_schema_spec):
    schema_loader.add_schema_spec(stream_schema_spec)
    window_bts_name = schema_loader.add_schema_spec(window_schema_spec)
    window_transformer_schema = WindowTransformerSchema(window_bts_name, schema_loader)
    anchor_spec = schema_loader.get_schema_spec('ProductMLExample.anchor')
    assert anchor_spec == window_schema_spec['Anchor']
    assert anchor_spec['Name'] == 'anchor'
    assert Type.is_type_equal(anchor_spec['Type'], Type.ANCHOR)
    assert isinstance(window_transformer_schema.anchor, AnchorSchema)


def test_evaluate_prepare_window_error(window_transformer, time_aggregate):
    time_aggregate.run_restore({
        'events': 3,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc).isoformat(),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc).isoformat()
    })
    with pytest.raises(
            PrepareWindowMissingBlocksError,
            match='last_session WindowAggregate: Expecting 1 but found 0 blocks'):
        window_transformer.run_evaluate(time_aggregate)


def test_evaluate_prepare_window(schema_loader, window_transformer, time_aggregate):
    init_memory_store(schema_loader.get_store('Sessions.memory'))
    time_aggregate.run_restore({
        'events': 3,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc).isoformat(),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc).isoformat()
    })
    assert window_transformer.run_evaluate(time_aggregate) is True


def test_evaluate_false(schema_loader, window_transformer, time_aggregate):
    init_memory_store(schema_loader.get_store('Sessions.memory'))
    time_aggregate.run_restore({
        'events': 0,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc).isoformat(),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc).isoformat()
    })
    assert window_transformer.run_evaluate(time_aggregate) is False


def test_evaluate_missing_block_error(window_transformer):
    with pytest.raises(
            TypeError, match='evaluate\(\) missing 1 required positional argument: \'block\''):
        window_transformer.run_evaluate()


def test_window_transformer(schema_loader, window_transformer, time_aggregate):
    init_memory_store(schema_loader.get_store('Sessions.memory'))

    time_aggregate.run_restore({
        'events': 3,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc).isoformat(),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc).isoformat()
    })

    assert window_transformer.run_evaluate(time_aggregate) is True

    snapshot = window_transformer._snapshot
    assert_aggregate_snapshot_equals(snapshot['last_session'], {'events': 2})
    assert_aggregate_snapshot_equals(snapshot['last_day'], {'total_events': 3})

    flattened_snapshot = window_transformer.run_flattened_snapshot
    del flattened_snapshot['last_session._identity']
    del flattened_snapshot['last_session._processed_tracker']
    del flattened_snapshot['last_day._identity']
    del flattened_snapshot['last_day._processed_tracker']

    assert flattened_snapshot == {
        'last_session.events': 2,
        'last_day.total_events': 3,
    }


def test_window_transformer_internal_reset(schema_loader, window_transformer, time_aggregate):
    init_memory_store(schema_loader.get_store('Sessions.memory'))
    window_transformer._anchor._schema.max = None

    time_aggregate.run_restore({
        'events': 3,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc).isoformat(),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc).isoformat()
    })

    assert window_transformer.run_evaluate(time_aggregate) is True
    snapshot = window_transformer._snapshot
    assert_aggregate_snapshot_equals(snapshot['last_session'], {'events': 2})
    # Total Events was 2 prior to exaclty once semantics.  I have tranced the execution and it seems correct.
    # TODO Validate this particular case
    assert_aggregate_snapshot_equals(snapshot['last_day'], {'total_events': 3})

    assert window_transformer.run_evaluate(time_aggregate) is True
    snapshot = window_transformer._snapshot
    assert_aggregate_snapshot_equals(snapshot['last_session'], {'events': 2})
    assert_aggregate_snapshot_equals(snapshot['last_day'], {'total_events': 3})


def test_window_transformer_schema_missing_attributes_adds_error(schema_loader, stream_schema_spec,
                                                                 window_schema_spec):
    del window_schema_spec[WindowTransformerSchema.ATTRIBUTE_ANCHOR]
    schema_loader.add_schema_spec(stream_schema_spec)
    window_bts_name = schema_loader.add_schema_spec(window_schema_spec)
    schema = WindowTransformerSchema(window_bts_name, schema_loader)

    assert 1 == len(schema.errors)
    assert isinstance(schema.errors[0], RequiredAttributeError)
    assert WindowTransformerSchema.ATTRIBUTE_ANCHOR == schema.errors[0].attribute
