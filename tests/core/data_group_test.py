from datetime import datetime, timezone
import yaml
from typing import Dict, Any
from pytest import fixture
import pytest

from blurr.core.evaluation import EvaluationContext
from blurr.core.data_group import DataGroupSchema, DataGroup
from blurr.core.errors import InvalidSchemaError
from blurr.core.field import Field
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Key
from blurr.store.memory_store import MemoryStore


def get_data_group_schema_spec() -> Dict[str, Any]:
    return {
        'Type': 'ProductML:DTC:DataGroup:SessionAggregate',
        'Name': 'user',
        'Store': 'memory',
        'Fields': [{
            'Name': 'event_count',
            'Type': 'integer',
            'Value': 5
        }]
    }


def get_store_spec() -> Dict[str, Any]:
    return {'Type': 'ProductML:DTC:Store:MemoryStore', 'Name': 'memory'}


class MockDataGroupSchema(DataGroupSchema):
    pass


class MockDataGroup(DataGroup):
    pass


@fixture
def data_group_schema_with_store():
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(get_data_group_schema_spec())
    schema_loader.add_schema(get_store_spec(), 'user')
    return MockDataGroupSchema(
        fully_qualified_name=name, schema_loader=schema_loader)


@fixture
def data_group_schema_without_store():
    schema_loader = SchemaLoader()
    data_group_schema_spec = get_data_group_schema_spec()
    del data_group_schema_spec['Store']
    name = schema_loader.add_schema(data_group_schema_spec)
    return MockDataGroupSchema(
        fully_qualified_name=name, schema_loader=schema_loader)


def test_data_group_initialization(data_group_schema_with_store):
    data_group = MockDataGroup(
        schema=data_group_schema_with_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    assert data_group.identity == "12345"


def test_data_group_nested_items(data_group_schema_with_store):
    data_group = MockDataGroup(
        schema=data_group_schema_with_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    nested_items = data_group.nested_items
    assert len(nested_items) == 1
    assert "event_count" in nested_items
    assert isinstance(nested_items["event_count"], Field)


def test_data_group_persist(data_group_schema_with_store,
                            data_group_schema_without_store):
    data_group = MockDataGroup(
        schema=data_group_schema_without_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    data_group.persist()

    data_group = MockDataGroup(
        schema=data_group_schema_with_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    dt = datetime.now()
    dt.replace(tzinfo=timezone.utc)
    data_group.persist(dt)
    snapshot_data_group = data_group.schema.store.get(
        Key(identity="12345", group="user", timestamp=dt))
    assert snapshot_data_group is not None
    assert snapshot_data_group == data_group.snapshot


def test_data_group_finalize(data_group_schema_with_store):
    data_group = MockDataGroup(
        schema=data_group_schema_with_store,
        identity="12345",
        evaluation_context=EvaluationContext())
    data_group.finalize()
    snapshot_data_group = data_group.schema.store.get(
        Key(identity="12345", group="user"))
    assert snapshot_data_group is not None
    assert snapshot_data_group == data_group.snapshot
