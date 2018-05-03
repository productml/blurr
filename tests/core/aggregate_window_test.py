from datetime import datetime, timezone

import pytest
from pytest import fixture

from blurr.core.aggregate_window import WindowAggregateSchema, WindowAggregate
from blurr.core.errors import PrepareWindowMissingBlocksError
from blurr.core.evaluation import EvaluationContext, Context
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type


@fixture
def window_aggregate_schema(schema_loader_with_mem_store: SchemaLoader, mem_store_name: str,
                            stream_dtc_name: str) -> WindowAggregateSchema:
    schema_loader_with_mem_store.add_schema_spec({
        'Type': Type.BLURR_AGGREGATE_BLOCK,
        'Name': 'session',
        'Store': mem_store_name,
        'Fields': [
            {
                'Name': 'events',
                'Type': Type.INTEGER,
                'Value': 'session.events + 1',
            },
        ],
    }, stream_dtc_name)
    name = schema_loader_with_mem_store.add_schema_spec({
        'Type': Type.BLURR_AGGREGATE_WINDOW,
        'Name': 'test_window_name',
        'WindowType': Type.DAY,
        'WindowValue': 1,
        'Source': stream_dtc_name + '.session',
        'Fields': [{
            'Name': 'total_events',
            'Type': Type.INTEGER,
            'Value': 'sum(source.events)'
        }]
    })

    return WindowAggregateSchema(name, schema_loader_with_mem_store)


@fixture
def window_aggregate(window_aggregate_schema: WindowAggregateSchema) -> WindowAggregate:
    return WindowAggregate(window_aggregate_schema, "user1",
                           EvaluationContext(Context({
                               'identity': 'user1'
                           })))


def test_window_type_day_positive(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.DAY
    window_aggregate._schema.window_value = 1
    window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == [4, 5]


def test_window_type_day_negative(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.DAY
    window_aggregate._schema.window_value = -1
    window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == [1, 2]


def test_window_type_day_zero_value(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.DAY
    window_aggregate._schema.window_value = 0
    with pytest.raises(PrepareWindowMissingBlocksError, match='No matching blocks found'):
        window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == []


def test_window_type_hour_positive(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.HOUR
    window_aggregate._schema.window_value = 1
    with pytest.raises(PrepareWindowMissingBlocksError, match='No matching blocks found'):
        window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == []

    window_aggregate._schema.window_type = Type.HOUR
    window_aggregate._schema.window_value = 2
    window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == [4]


def test_window_type_hour_negative(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.HOUR
    window_aggregate._schema.window_value = -24
    window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == [1, 2]


def test_window_type_hour_zero_value(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.HOUR
    window_aggregate._schema.window_value = 0
    with pytest.raises(
            PrepareWindowMissingBlocksError,
            match='test_window_name WindowAggregate: No matching blocks found'):
        window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == []


def test_window_type_count_positive(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.COUNT
    window_aggregate._schema.window_value = -1
    window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == [2]


def test_window_type_count_negative(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.COUNT
    window_aggregate._schema.window_value = 1
    window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == [4]


def test_window_type_count_missing_sesssions(window_aggregate: WindowAggregate) -> None:
    window_aggregate._schema.window_type = Type.COUNT
    window_aggregate._schema.window_value = 20
    with pytest.raises(
            PrepareWindowMissingBlocksError,
            match='test_window_name WindowAggregate: Expecting 20 but found 3 blocks'):
        window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window_aggregate._window_source.events == [4, 5, 6]


def test_evaluate(window_aggregate):
    window_aggregate._schema.window_type = Type.DAY
    window_aggregate._schema.window_value = 1
    window_aggregate._prepare_window(datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    window_aggregate.run_evaluate()
    assert window_aggregate.total_events == 9
