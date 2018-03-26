from datetime import datetime, timezone

import pytest
from pytest import fixture

from blurr.core.errors import PrepareWindowMissingSessionsError
from blurr.core.schema_loader import SchemaLoader
from blurr.core.window import WindowSchema, Window


@fixture
def window_schema(schema_loader_with_mem_store: SchemaLoader,
                  mem_store_name: str, stream_dtc_name: str) -> WindowSchema:
    schema_loader_with_mem_store.add_schema({
        'Type': 'ProductML:DTC:DataGroup:SessionAggregate',
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
        'Name': 'source',
        'Type': 'day',
        'Value': 1,
        'Source': stream_dtc_name + '.session',
    })
    return WindowSchema(name, schema_loader_with_mem_store)


@fixture
def window(window_schema: WindowSchema) -> Window:
    return Window(window_schema)


def test_window_type_day(window: Window) -> None:
    window.schema.type = 'day'
    window.schema.value = 1
    window.prepare('user1', datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == [4, 5]

    window.schema.type = 'day'
    window.schema.value = -1
    window.prepare('user1', datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == [1, 2]

    window.schema.type = 'day'
    window.schema.value = 0
    with pytest.raises(
            PrepareWindowMissingSessionsError,
            match='No matching sessions found'):
        window.prepare('user1',
                       datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == []


def test_window_type_hour(window: Window) -> None:
    window.schema.type = 'hour'
    window.schema.value = 1
    with pytest.raises(
            PrepareWindowMissingSessionsError,
            match='No matching sessions found'):
        window.prepare('user1',
                       datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == []

    window.schema.type = 'hour'
    window.schema.value = 2
    window.prepare('user1', datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == [4]

    window.schema.type = 'hour'
    window.schema.value = -24
    window.prepare('user1', datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == [1, 2]

    window.schema.type = 'hour'
    window.schema.value = 0
    with pytest.raises(
            PrepareWindowMissingSessionsError,
            match='No matching sessions found'):
        window.prepare('user1',
                       datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == []


def test_window_type_count(window: Window) -> None:
    window.schema.type = 'count'
    window.schema.value = -1
    window.prepare('user1', datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == [2]

    window.schema.type = 'count'
    window.schema.value = 1
    window.prepare('user1', datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == [4]

    window.schema.type = 'count'
    window.schema.value = 20
    with pytest.raises(
            PrepareWindowMissingSessionsError,
            match='Expecting 20 but not found 3 sessions'):
        window.prepare('user1',
                       datetime(2018, 3, 7, 21, 36, 31, 0, timezone.utc))
    assert window.events == [4, 5, 6]
