from datetime import datetime, timezone
from typing import Dict, Any

import pytest
import yaml

from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroup, \
    SessionDataGroupSchema
from blurr.core.window_transformer import WindowTransformer, \
    WindowTransformerSchema
from blurr.store.memory_store import MemoryStore


@pytest.fixture
def test_stream_schema_spec():
    return yaml.safe_load(open('tests/data/stream.yml'))


@pytest.fixture
def test_window_schema_spec():
    return yaml.safe_load(open('tests/data/window.yml'))


@pytest.fixture()
def state() -> Dict[str, Any]:
    return {
        'user1/state': {
            'variable_1': 1,
            'variable_a': 'a',
            'variable_true': True
        },
        'user1/session/2018-03-07T22:35:31+00:00': {
            'events': 1,
            'start_time': datetime(2018, 3, 7, 22, 35, 31, 0, timezone.utc)
        },
        'user1/session/2018-03-07T22:35:35+00:00': {
            'events': 2,
            'start_time': datetime(2018, 3, 7, 22, 35, 35, 0, timezone.utc)
        },
        'user1/session/2018-03-07T22:36:31+00:00': {
            'events': 3,
            'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc)
        },
        'user1/session/2018-03-07T22:38:31+00:00': {
            'events': 4,
            'start_time': datetime(2018, 3, 7, 22, 38, 31, 0, timezone.utc)
        },
        'user1/session/2018-03-07T22:40:31+00:00': {
            'events': 5,
            'start_time': datetime(2018, 3, 7, 22, 40, 31, 0, timezone.utc)
        }
    }


def test_window_transformer(test_stream_schema_spec, test_window_schema_spec,
                            state):
    store = MemoryStore(state)
    schema_loader = SchemaLoader()
    stream_dtc_name = schema_loader.add_schema(test_stream_schema_spec)
    window_dtc_name = schema_loader.add_schema(test_window_schema_spec)
    window_transformer = WindowTransformer(store,
                                           WindowTransformerSchema(
                                               window_dtc_name, schema_loader),
                                           'user1', Context())
    session = SessionDataGroup(
        SessionDataGroupSchema(stream_dtc_name + '.session', schema_loader),
        EvaluationContext())
    session.restore({
        'events': 3,
        'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc),
        'end_time': datetime(2018, 3, 7, 22, 37, 31, 0, timezone.utc)
    })

    assert window_transformer.evaluate_anchor(session) == True

    snapshot = window_transformer.snapshot
    assert snapshot['last_session'] == {'events': 2}
    assert snapshot['last_day'] == {'total_events': 3}
