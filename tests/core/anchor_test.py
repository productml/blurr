from datetime import datetime, timezone

from pytest import fixture

from blurr.core.anchor import AnchorSchema, Anchor
from blurr.core.evaluation import EvaluationContext, Expression
from blurr.core.schema_loader import SchemaLoader
from blurr.core.aggregate_block import BlockAggregateSchema, \
    BlockAggregate


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
def block_schema(schema_loader: SchemaLoader) -> BlockAggregateSchema:
    name = schema_loader.add_schema({
        'Type': 'Blurr:Aggregate:BlockAggregate',
        'Name': 'session',
        'Fields': [
            {
                'Name': 'events',
                'Type': 'integer',
                'Value': 'session.events + 1',
            },
        ],
    })
    return BlockAggregateSchema(name, schema_loader)


@fixture
def block_item(block_schema: BlockAggregateSchema) -> BlockAggregate:
    block = BlockAggregate(block_schema, 'user1', EvaluationContext())
    block.restore({
        'events': 3,
        '_start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc),
        '_end_time': datetime(2018, 3, 7, 22, 37, 31, 0, timezone.utc)
    })
    return block


def test_anchor_max_one(anchor_schema_max_one: AnchorSchema, block_item: BlockAggregate) -> None:
    anchor = Anchor(anchor_schema_max_one, EvaluationContext())
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is False


def test_anchor_max_two(anchor_schema_max_two: AnchorSchema, block_item: BlockAggregate) -> None:
    anchor = Anchor(anchor_schema_max_two, EvaluationContext())
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is False


def test_anchor_max_not_specified(anchor_schema_max_one: AnchorSchema,
                                  block_item: BlockAggregate) -> None:
    anchor_schema_max_one.max = None
    anchor = Anchor(anchor_schema_max_one, EvaluationContext())
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is True


def test_anchor_condition(anchor_schema_max_one: AnchorSchema, block_item: BlockAggregate) -> None:
    anchor_schema_max_one.condition = Expression('session.events > 3')
    eval_context = EvaluationContext()
    anchor = Anchor(anchor_schema_max_one, eval_context)
    eval_context.local_context.add(block_item._schema.fully_qualified_name, block_item)
    assert anchor.evaluate_anchor(block_item) is False

    block_item.restore({
        'events': 4,
        '_start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc),
        '_end_time': datetime(2018, 3, 7, 22, 37, 31, 0, timezone.utc)
    })
    assert anchor.evaluate_anchor(block_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(block_item) is False
