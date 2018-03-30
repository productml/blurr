from typing import Dict, Any

from pytest import fixture

from blurr.core.evaluation import Expression
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroupSchema


@fixture
def session_data_group_schema_spec() -> Dict[str, Any]:
    return {
        'Type': 'ProductML:DTC:DataGroup:SessionAggregate',
        'Name': 'user',
        'Filter': 'source.event_id in ["app_launched", "user_updated"]',
        'Fields': [{
            'Name': 'event_count',
            'Type': 'integer',
            'Value': 5
        }]
    }


def match_fields(fields: Dict[str, Any]) -> bool:
    expected_fields = [{
        'Name': 'start_time',
        'Type': 'datetime',
        'Value': 'time if user.start_time is None else time if time < '
        'user.start_time else user.start_time'
    }, {
        'Name': 'end_time',
        'Type': 'datetime',
        'Value': 'time if user.end_time is None else time if time > '
        'user.end_time else user.end_time'
    }, {
        'Name': 'event_count',
        'Type': 'integer',
        'Value': 5
    }]
    return fields == expected_fields


def test_session_data_group_schema_initialization(
        session_data_group_schema_spec: Dict[str, Any]) -> None:
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(session_data_group_schema_spec)
    session_data_group_schema = SessionDataGroupSchema(name, schema_loader)
    assert session_data_group_schema.split is None
    assert match_fields(session_data_group_schema_spec['Fields'])

    loader_spec = schema_loader.get_schema_spec(name)
    assert match_fields(loader_spec['Fields'])


def test_session_data_group_schema_with_split_initialization(
        session_data_group_schema_spec: Dict[str, Any]) -> None:
    session_data_group_schema_spec['Split'] = '4 > 2'
    schema_loader = SchemaLoader()
    name = schema_loader.add_schema(session_data_group_schema_spec)
    session_data_group_schema = SessionDataGroupSchema(name, schema_loader)
    assert isinstance(session_data_group_schema.split, Expression)
    assert match_fields(session_data_group_schema_spec['Fields'])

    loader_spec = schema_loader.get_schema_spec(name)
    assert match_fields(loader_spec['Fields'])
