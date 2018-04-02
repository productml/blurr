from datetime import datetime, timezone
from typing import Dict, Any

from pytest import fixture

from blurr.core.evaluation import EvaluationContext
from blurr.core.field import Field
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroupSchema, \
    SessionDataGroup
from blurr.core.store import Key


@fixture
def session_data_group_schema_spec() -> Dict[str, Any]:
    return {
        'Type': 'ProductML:DTC:DataGroup:SessionAggregate',
        'Name': 'user',
        'Store': 'memstore',
        'Fields': [{
            'Name': 'event_count',
            'Type': 'integer',
            'Value': 'user.event_count + 1'
        }]
    }


@fixture
def store_spec() -> Dict[str, Any]:
    return {'Name': 'memstore', 'Type': 'ProductML:DTC:Store:MemoryStore'}


@fixture
def schema_loader(store_spec) -> SchemaLoader:
    schema_loader = SchemaLoader()
    schema_loader.add_schema(store_spec, 'user')
    return schema_loader


def check_fields(fields: Dict[str, Field],
                 expected_field_values: Dict[str, Any]) -> bool:
    if len(fields) != len(expected_field_values):
        return False

    for field_name, field in fields.items():
        if not isinstance(field, Field):
            return False
        if field.value != expected_field_values[field_name]:
            return False

    return True


def create_session_data_group(schema, time) -> SessionDataGroup:
    evaluation_context = EvaluationContext()
    session_data_group = SessionDataGroup(
        schema=schema, identity='12345', evaluation_context=evaluation_context)
    evaluation_context.global_add('time', time)
    evaluation_context.global_add('user', session_data_group)
    return session_data_group


def test_session_data_group_schema_evaluate_without_split(
        session_data_group_schema_spec, schema_loader):
    name = schema_loader.add_schema(session_data_group_schema_spec)
    session_data_group_schema = SessionDataGroupSchema(name, schema_loader)

    time = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    session_data_group = create_session_data_group(session_data_group_schema,
                                                   time)
    session_data_group.evaluate()

    # Check eval results of various fields
    assert len(session_data_group.nested_items) == 3
    assert check_fields(session_data_group.nested_items, {
        'event_count': 1,
        'start_time': time,
        'end_time': time
    })

    # aggregate snapshot should not exist in store
    assert session_data_group_schema.store.get(
        Key(identity=session_data_group.identity,
            group=session_data_group.name,
            timestamp=session_data_group.start_time)) is None


def test_session_data_group_schema_evaluate_with_split(
        session_data_group_schema_spec, schema_loader):
    session_data_group_schema_spec['Split'] = 'user.event_count == 2'
    name = schema_loader.add_schema(session_data_group_schema_spec)
    session_data_group_schema = SessionDataGroupSchema(name, schema_loader)

    time = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    session_data_group = create_session_data_group(session_data_group_schema,
                                                   time)
    session_data_group.evaluate()
    session_data_group.evaluate()

    # Check eval results of various fields before split
    assert check_fields(session_data_group.nested_items, {
        'event_count': 2,
        'start_time': time,
        'end_time': time
    })

    current_snapshot = session_data_group.snapshot
    session_data_group.evaluate()

    # Check eval results of various fields
    assert check_fields(session_data_group.nested_items, {
        'event_count': 1,
        'start_time': time,
        'end_time': time
    })

    # Check aggregate snapshot present in store
    assert session_data_group_schema.store.get(
        Key(identity=session_data_group.identity,
            group=session_data_group.name,
            timestamp=time)) == current_snapshot
