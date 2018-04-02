from datetime import datetime, timezone
from typing import Dict, Any

from pytest import fixture

from blurr.core.evaluation import EvaluationContext
from blurr.core.field import Field
from blurr.core.schema_loader import SchemaLoader
from blurr.core.block_data_group import BlockDataGroupSchema, \
    BlockDataGroup
from blurr.core.store import Key


@fixture
def block_data_group_schema_spec() -> Dict[str, Any]:
    return {
        'Type': 'Blurr:DataGroup:BlockAggregate',
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
    return {'Name': 'memstore', 'Type': 'Blurr:Store:MemoryStore'}


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


def create_block_data_group(schema, time) -> BlockDataGroup:
    evaluation_context = EvaluationContext()
    block_data_group = BlockDataGroup(
        schema=schema, identity='12345', evaluation_context=evaluation_context)
    evaluation_context.global_add('time', time)
    evaluation_context.global_add('user', block_data_group)
    return block_data_group


def test_block_data_group_schema_evaluate_without_split(
        block_data_group_schema_spec, schema_loader):
    name = schema_loader.add_schema(block_data_group_schema_spec)
    block_data_group_schema = BlockDataGroupSchema(name, schema_loader)

    time = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    block_data_group = create_block_data_group(block_data_group_schema, time)
    block_data_group.evaluate()

    # Check eval results of various fields
    assert len(block_data_group.nested_items) == 3
    assert check_fields(block_data_group.nested_items, {
        'event_count': 1,
        'start_time': time,
        'end_time': time
    })

    # aggregate snapshot should not exist in store
    assert block_data_group_schema.store.get(
        Key(identity=block_data_group.identity,
            group=block_data_group.name,
            timestamp=block_data_group.start_time)) is None


def test_block_data_group_schema_evaluate_with_split(
        block_data_group_schema_spec, schema_loader):
    block_data_group_schema_spec['Split'] = 'user.event_count == 2'
    name = schema_loader.add_schema(block_data_group_schema_spec)
    block_data_group_schema = BlockDataGroupSchema(name, schema_loader)

    time = datetime(2018, 3, 7, 19, 35, 31, 0, timezone.utc)
    block_data_group = create_block_data_group(block_data_group_schema, time)
    block_data_group.evaluate()
    block_data_group.evaluate()

    # Check eval results of various fields before split
    assert check_fields(block_data_group.nested_items, {
        'event_count': 2,
        'start_time': time,
        'end_time': time
    })

    current_snapshot = block_data_group.snapshot
    block_data_group.evaluate()

    # Check eval results of various fields
    assert check_fields(block_data_group.nested_items, {
        'event_count': 1,
        'start_time': time,
        'end_time': time
    })

    # Check aggregate snapshot present in store
    assert block_data_group_schema.store.get(
        Key(identity=block_data_group.identity,
            group=block_data_group.name,
            timestamp=time)) == current_snapshot
