from datetime import datetime, timezone

import pytest
import yaml
from pytest import fixture

from blurr.core.anchor import AnchorSchema
from blurr.core.errors import AnchorBlockNotDefinedError, PrepareWindowMissingBlocksError
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.aggregate_block import BlockAggregate, \
    BlockAggregateSchema
from blurr.core.streaming_transformer import StreamingTransformer
from blurr.core.window_transformer import WindowTransformer, \
    WindowTransformerSchema
from tests.core.conftest import init_memory_store


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
    stream_dtc_name = schema_loader.add_schema(stream_schema_spec)
    return StreamingTransformer(
        schema_loader.get_schema_object(stream_dtc_name), 'user1', Context())


@fixture
def window_transformer(schema_loader, stream_transformer, window_schema_spec):
    window_dtc_name = schema_loader.add_schema(window_schema_spec)
    return WindowTransformer(
        schema_loader.get_schema_object(window_dtc_name), 'user1',
        Context({
            stream_transformer._schema.name: stream_transformer
        }))


@fixture
def block_aggregate(stream_transformer):
    block = None
    for data_group in stream_transformer._nested_items.values():
        if isinstance(data_group, BlockAggregate):
            block = data_group

    return block


def test_window_transformer_schema_init(schema_loader, stream_schema_spec, window_schema_spec):
    schema_loader.add_schema(stream_schema_spec)
    window_dtc_name = schema_loader.add_schema(window_schema_spec)
    window_transformer_schema = WindowTransformerSchema(window_dtc_name, schema_loader)
    anchor_spec = schema_loader.get_schema_spec('ProductMLExample.anchor')
    assert anchor_spec == window_schema_spec['Anchor']
    assert anchor_spec['Name'] == 'anchor'
    assert anchor_spec['Type'] == 'anchor'
    assert isinstance(window_transformer_schema.anchor, AnchorSchema)


def test_evaluate_anchor_prepare_window_error(window_transformer, block_aggregate):
    block_aggregate.restore({
        'events': 3,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc)
    })
    with pytest.raises(
            PrepareWindowMissingBlocksError,
            match='last_session WindowAggregate: Expecting 1 but found 0 blocks'):
        window_transformer.evaluate_anchor(block_aggregate)


def test_evaluate_anchor_prepare_window(schema_loader, window_transformer, block_aggregate):
    init_memory_store(schema_loader.get_schema_object('Sessions.memory'))
    block_aggregate.restore({
        'events': 3,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc)
    })
    assert window_transformer.evaluate_anchor(block_aggregate) is True


def test_evaluate_anchor_false(schema_loader, window_transformer, block_aggregate):
    init_memory_store(schema_loader.get_schema_object('Sessions.memory'))
    block_aggregate.restore({
        'events': 0,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc)
    })
    assert window_transformer.evaluate_anchor(block_aggregate) is False


def test_evaluate_error(window_transformer):
    with pytest.raises(
            AnchorBlockNotDefinedError,
            match=('WindowTransformer does not support evaluate directly.'
                   ' Call evaluate_anchor instead.')):
        window_transformer.evaluate()


def test_window_transformer(schema_loader, window_transformer, block_aggregate):
    init_memory_store(schema_loader.get_schema_object('Sessions.memory'))

    block_aggregate.restore({
        'events': 3,
        '_start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc),
        '_end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc)
    })

    assert window_transformer.evaluate_anchor(block_aggregate) is True

    snapshot = window_transformer._snapshot
    assert snapshot['last_session'] == {'_identity': 'user1', 'events': 2}
    assert snapshot['last_day'] == {'_identity': 'user1', 'total_events': 3}

    assert window_transformer.flattened_snapshot == {
        'last_session.events': 2,
        'last_session._identity': 'user1',
        'last_day.total_events': 3,
        'last_day._identity': 'user1'
    }
