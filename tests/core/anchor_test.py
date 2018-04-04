from datetime import datetime, timezone

from pytest import fixture

from blurr.core.anchor import AnchorSchema, Anchor
from blurr.core.evaluation import EvaluationContext, Expression
from blurr.core.schema_loader import SchemaLoader
from blurr.core.block_data_group import BlockDataGroupSchema, \
    BlockDataGroup


@fixture
def schema_loader():
    return SchemaLoader()


@fixture
def anchor_schema_max_one(schema_loader: SchemaLoader) -> AnchorSchema:
    name = schema_loader.add_schema({
        'Condition': True,
        'Max': 1,
        'Name': 'anchor',
        'Type': 'anchor'
    })
    return AnchorSchema(name, schema_loader)


@fixture
def anchor_schema_max_two(schema_loader: SchemaLoader) -> AnchorSchema:
    name = schema_loader.add_schema({
        'Condition': True,
        'Max': 2,
        'Name': 'anchor',
        'Type': 'anchor'
    })
    return AnchorSchema(name, schema_loader)


@fixture
def block_schema(schema_loader: SchemaLoader) -> BlockDataGroupSchema:
    name = schema_loader.add_schema({
        'Type': 'Blurr:DataGroup:BlockAggregate',
        'Name': 'session',
        'Fields': [
            {
                'Name': 'events',
                'Type': 'integer',
                'Value': 'session.events + 1',
            },
        ],
    })
    return BlockDataGroupSchema(name, schema_loader)


@fixture
def block_item(block_schema: BlockDataGroupSchema) -> BlockDataGroup:
    block = BlockDataGroup(block_schema, 'user1', EvaluationContext())
    block.restore({
        'events': 3,
        'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc),
        'end_time': datetime(2018, 3, 7, 22, 37, 31, 0, timezone.utc)
    })
    return block


def test_anchor_max_one(anchor_schema_max_one: AnchorSchema, block_item: BlockDataGroup) -> None:
    anchor = Anchor(anchor_schema_max_one, EvaluationContext())
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is False


def test_anchor_max_two(anchor_schema_max_two: AnchorSchema, block_item: BlockDataGroup) -> None:
    anchor = Anchor(anchor_schema_max_two, EvaluationContext())
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is False


def test_anchor_max_not_specified(anchor_schema_max_one: AnchorSchema,
                                  block_item: BlockDataGroup) -> None:
    anchor_schema_max_one.max = None
    anchor = Anchor(anchor_schema_max_one, EvaluationContext())
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is True


def test_anchor_condition(anchor_schema_max_one: AnchorSchema, block_item: BlockDataGroup) -> None:
    anchor_schema_max_one.condition = Expression('session.events > 3')
    eval_context = EvaluationContext()
    anchor = Anchor(anchor_schema_max_one, eval_context)
    eval_context.local_context.add(block_item.schema.fully_qualified_name, block_item)
    assert anchor.evaluate_anchor(block_item) is False

    block_item.restore({
        'events': 4,
        'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc),
        'end_time': datetime(2018, 3, 7, 22, 37, 31, 0, timezone.utc)
    })
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is False
