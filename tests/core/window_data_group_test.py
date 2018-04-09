from datetime import datetime, timezone

import pytest
from pytest import fixture

from blurr.core.errors import PrepareWindowMissingBlocksError
from blurr.core.evaluation import EvaluationContext, Context
from blurr.core.schema_loader import SchemaLoader
from blurr.core.window_data_group import WindowDataGroupSchema, WindowDataGroup


@fixture
def window_data_group_schema(schema_loader_with_mem_store: SchemaLoader, mem_store_name: str,
                             stream_dtc_name: str) -> WindowDataGroupSchema:
    schema_loader_with_mem_store.add_schema({
        'Type': 'Blurr:DataGroup:BlockAggregate',
        'Name': 'session',
        'Store': mem_store_name,
        'Fields': [
            {
                'Name': 'events',
                'Type': 'integer',
                'Value': 'session.events + 1',
            },
        ],
    }, stream_dtc_name)
    name = schema_loader_with_mem_store.add_schema({
        'Type': 'ProductML:DTC:DataGroup:WindowAggregate',
        'Name': 'test_window_name',
        'WindowType': 'day',
        'WindowValue': 1,
        'Source': stream_dtc_name + '.session',
        'Fields': [{
            'Name': 'total_events',
            'Type': 'integer',
            'Value': 'sum(source.events)'
        }]
    })

    return WindowDataGroupSchema(name, schema_loader_with_mem_store)


@fixture
def window_data_group(window_data_group_schema: WindowDataGroupSchema) -> WindowDataGroup:
    return WindowDataGroup(window_data_group_schema, "user1",
                           EvaluationContext(Context({
                               'identity': 'user1'
                           })))


def test_window_type_day_positive(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'day'
    window_data_group._schema.window_value = 1
    window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == [4, 5]


def test_window_type_day_negative(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'day'
    window_data_group._schema.window_value = -1
    window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == [1, 2]


def test_window_type_day_zero_value(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'day'
    window_data_group._schema.window_value = 0
    with pytest.raises(PrepareWindowMissingBlocksError, match='No matching blocks found'):
        window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == []


def test_window_type_hour_positive(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'hour'
    window_data_group._schema.window_value = 1
    with pytest.raises(PrepareWindowMissingBlocksError, match='No matching blocks found'):
        window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == []

    window_data_group._schema.window_type = 'hour'
    window_data_group._schema.window_value = 2
    window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == [4]


def test_window_type_hour_negative(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'hour'
    window_data_group._schema.window_value = -24
    window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == [1, 2]


def test_window_type_hour_zero_value(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'hour'
    window_data_group._schema.window_value = 0
    with pytest.raises(
            PrepareWindowMissingBlocksError,
            match='test_window_name WindowAggregate: No matching blocks found'):
        window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == []


def test_window_type_count_positive(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'count'
    window_data_group._schema.window_value = -1
    window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == [2]


def test_window_type_count_negative(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'count'
    window_data_group._schema.window_value = 1
    window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == [4]


def test_window_type_count_missing_sesssions(window_data_group: WindowDataGroup) -> None:
    window_data_group._schema.window_type = 'count'
    window_data_group._schema.window_value = 20
    with pytest.raises(
            PrepareWindowMissingBlocksError,
            match='test_window_name WindowAggregate: Expecting 20 but found 3 blocks'):
        window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_data_group._window_source.events == [4, 5, 6]


def test_evaluate(window_data_group):
    window_data_group._schema.window_type = 'day'
    window_data_group._schema.window_value = 1
    window_data_group.prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    window_data_group.evaluate()
    assert window_data_group.total_events == 9
