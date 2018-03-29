from datetime import datetime, timezone

from pytest import fixture

from blurr.core.anchor import AnchorSchema, Anchor
from blurr.core.evaluation import EvaluationContext, Expression
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroupSchema, \
    SessionDataGroup


@fixture
def schema_loader() -> SchemaLoader:
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
def session_schema(schema_loader: SchemaLoader) -> SessionDataGroupSchema:
    name = schema_loader.add_schema({
        'Type': 'ProductML:DTC:DataGroup:SessionAggregate',
        'Name': 'session',
        'Fields': [
            {
                'Name': 'events',
                'Type': 'integer',
                'Value': 'session.events + 1',
            },
        ],
    })
    return SessionDataGroupSchema(name, schema_loader)


@fixture
def session_item(session_schema: SessionDataGroupSchema) -> SessionDataGroup:
    session = SessionDataGroup(session_schema, 'user1', EvaluationContext())
    session.restore({
        'events': 3,
        'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc),
        'end_time': datetime(2018, 3, 7, 22, 37, 31, 0, timezone.utc)
    })
    return session


def test_anchor_max_one(anchor_schema_max_one: AnchorSchema,
                        session_item: SessionDataGroup) -> None:
    anchor = Anchor(anchor_schema_max_one, EvaluationContext())
    assert anchor.evaluate_anchor(session_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(session_item) is False


def test_anchor_max_two(anchor_schema_max_two: AnchorSchema,
                        session_item: SessionDataGroup) -> None:
    anchor = Anchor(anchor_schema_max_two, EvaluationContext())
    assert anchor.evaluate_anchor(session_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(session_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(session_item) is False


def test_anchor_condition(anchor_schema_max_one: AnchorSchema,
                          session_item: SessionDataGroup) -> None:
    anchor_schema_max_one.condition = Expression('session.events > 3')
    eval_context = EvaluationContext()
    anchor = Anchor(anchor_schema_max_one, eval_context)
    eval_context.local_context.add(session_item.schema.fully_qualified_name,
                                   session_item)
    assert anchor.evaluate_anchor(session_item) is False

    session_item.restore({
        'events': 4,
        'start_time': datetime(2018, 3, 7, 22, 36, 31, 0, timezone.utc),
        'end_time': datetime(2018, 3, 7, 22, 37, 31, 0, timezone.utc)
    })
    assert anchor.evaluate_anchor(session_item) is True
    anchor.add_condition_met()
    assert anchor.evaluate_anchor(session_item) is False
