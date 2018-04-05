from datetime import datetime, timezone

import yaml
from pytest import fixture

from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.block_data_group import BlockDataGroup, \
    BlockDataGroupSchema
from blurr.core.streaming_transformer import StreamingTransformer
from blurr.core.window_transformer import WindowTransformer, \
    WindowTransformerSchema
from tests.core.conftest import init_memory_store


@fixture
def test_stream_schema_spec():
    return yaml.safe_load(open('tests/data/stream.yml'))


@fixture
def test_window_schema_spec():
    return yaml.safe_load(open('tests/data/window.yml'))


def test_window_transformer(test_stream_schema_spec, test_window_schema_spec):
    schema_loader = SchemaLoader()
    stream_dtc_name = schema_loader.add_schema(test_stream_schema_spec)
    window_dtc_name = schema_loader.add_schema(test_window_schema_spec)
    init_memory_store(schema_loader.get_schema_object('Sessions.memory'))

    stream_transformer = StreamingTransformer(
        schema_loader.get_schema_object(stream_dtc_name), 'user1', Context())

    block = None
    for _, data_group in stream_transformer.nested_items.items():
        if isinstance(data_group, BlockDataGroup):
            block = data_group
    if block is None:
        return []

    window_transformer = WindowTransformer(
        WindowTransformerSchema(window_dtc_name, schema_loader), 'user1',
        Context({
            stream_transformer.schema.name: stream_transformer
        }))

    block.restore({
        'events': 3,
        'start_time': datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc),
        'end_time': datetime(2018, 3, 7, 21, 37, 31, 0, timezone.utc)
    })

    assert window_transformer.evaluate_anchor(block) is True

    snapshot = window_transformer.snapshot
    assert snapshot['last_session'] == {'identity': 'user1', 'events': 2}
    assert snapshot['last_day'] == {'identity': 'user1', 'total_events': 3}

    assert window_transformer.flattened_snapshot == {
        'last_session.events': 2,
        'last_session.identity': 'user1',
        'last_day.total_events': 3,
        'last_day.identity': 'user1'
    }
