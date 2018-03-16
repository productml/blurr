from datetime import datetime

import pytest
import yaml

from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.session_data_group import SessionDataGroup, \
    SessionDataGroupSchema
from blurr.core.store import Store
from blurr.core.window_transformer import WindowTransformer, \
    WindowTransformerSchema


@pytest.fixture
def test_stream_schema_spec():
    return yaml.safe_load(open('tests/data/stream.yml'))


@pytest.fixture
def test_window_schema_spec():
    return yaml.safe_load(open('tests/data/window.yml'))


class TestStore(Store):
    def __init__(self, stream_spec):
        super().__init__()
        self.stream_spec = stream_spec

    def _store_get(self, identity, group):
        pass

    def get_window_by_time(self, identity, group_id, start_time, end_time):
        session1 = SessionDataGroup(
            SessionDataGroupSchema(self.stream_spec['DataGroups'][0]),
            EvaluationContext())
        session1.restore({
            'events': 1,
            'start_time': datetime(2018, 1, 1, 1),
            'end_time': datetime(2018, 1, 1, 2)
        })
        session2 = SessionDataGroup(
            SessionDataGroupSchema(self.stream_spec['DataGroups'][0]),
            EvaluationContext())
        session2.restore({
            'events': 1,
            'start_time': datetime(2018, 1, 1, 1),
            'end_time': datetime(2018, 1, 1, 2)
        })
        return [session1, session2]

    def get_window_by_count(self, identity, group_id, start_time, count):
        session = SessionDataGroup(
            SessionDataGroupSchema(self.stream_spec['DataGroups'][0]),
            EvaluationContext())
        session.restore({
            'events': 3,
            'start_time': datetime(2018, 1, 1, 1),
            'end_time': datetime(2018, 1, 1, 2)
        })
        return [session]


def test_window_transformer(test_stream_schema_spec, test_window_schema_spec):
    window_transformer = WindowTransformer(
        TestStore(test_stream_schema_spec),
        WindowTransformerSchema(test_window_schema_spec), 'user1', Context())
    session = SessionDataGroup(
        SessionDataGroupSchema(test_stream_schema_spec['DataGroups'][0]),
        EvaluationContext())
    session.restore({
        'events': 1,
        'start_time': datetime(2018, 1, 1, 1),
        'end_time': datetime(2018, 1, 1, 2)
    })

    assert window_transformer.evaluate_anchor(session) == True

    snapshot = window_transformer.snapshot
    assert snapshot['last_session'] == {'events': 1}
    assert snapshot['last_day'] == {'total_events': 2}
