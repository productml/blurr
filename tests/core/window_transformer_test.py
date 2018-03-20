from datetime import datetime, timezone

import yaml
from pytest import fixture

from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroup, \
    SessionDataGroupSchema
from blurr.core.store import Key
from blurr.core.window_transformer import WindowTransformer, \
    WindowTransformerSchema
from blurr.store.memory_store import MemoryStore


@fixture
def test_stream_schema_spec():
    return yaml.safe_load(open('tests/data/stream.yml'))


@fixture
def test_window_schema_spec():
    return yaml.safe_load(open('tests/data/window.yml'))


def init_memory_store(store: MemoryStore) -> None:
    store.save(
        Key('user1', 'state'), {
            'variable_1': 1,
            'variable_a': 'a',
            'variable_true': True
        })

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc)), {
                'events': 1,
                'start_time': datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc)
            })

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 35, 35, 0, timezone.utc)), {
                'events': 2,
                'start_time': datetime(2018, 3, 7, 22, 35, 35, 0, timezone.utc)
            })

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc)), {
                'events': 3,
                'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc)
            })

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc)), {
                'events': 4,
                'start_time': datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc)
            })

    store.save(
        Key('user1', 'session',
            datetime(2018, 3, 7, 22, 40, 31, 0, timezone.utc)), {
                'events': 5,
                'start_time': datetime(2018, 3, 7, 22, 40, 31, 0, timezone.utc)
            })


def test_window_transformer(test_stream_schema_spec, test_window_schema_spec):
    schema_loader = SchemaLoader()
    stream_dtc_name = schema_loader.add_schema(test_stream_schema_spec)
    window_dtc_name = schema_loader.add_schema(test_window_schema_spec)
    init_memory_store(schema_loader.get_schema_object('Sessions.memory'))

    window_transformer = WindowTransformer(
        WindowTransformerSchema(window_dtc_name, schema_loader), 'user1',
        Context())
    session = SessionDataGroup(
        SessionDataGroupSchema(stream_dtc_name + '.session', schema_loader),
        'user1', EvaluationContext())
    session.restore({
        'events': 3,
        'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc),
        'end_time': datetime(2018, 3, 7, 22, 37, 31, 0, timezone.utc)
    })

    assert window_transformer.evaluate_anchor(session) == True

    snapshot = window_transformer.snapshot
    assert snapshot['last_session'] == {'events': 2}
    assert snapshot['last_day'] == {'total_events': 3}
