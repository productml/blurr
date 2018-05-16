import logging
from typing import Dict

import pytest
from pytest import raises, fixture

from blurr.core.base import Expression
from blurr.core.errors import MissingAttributeError
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.transformer_streaming import StreamingTransformer
from blurr.core.type import Type


@fixture
def schema_spec() -> Dict:
    return {
        'Name': 'test',
        'Type': Type.BLURR_TRANSFORM_STREAMING,
        "Version": "2018-03-01",
        "Time": "parser.parse(source.event_time)",
        "Identity": "source.user_id",
        'Aggregates': [{
            'Name': 'test_group',
            'Type': Type.BLURR_AGGREGATE_IDENTITY,
            'Fields': [{
                "Type": "integer",
                "Name": "events",
                "Value": "test_group.events+1"
            }]
        }]
    }


@fixture
def schema_loader(schema_spec: Dict) -> SchemaLoader:
    schema_loader = SchemaLoader()
    schema_loader.add_schema_spec(schema_spec)
    return schema_loader


def test_expression_valid_single_value() -> None:
    code_string = 1
    expr = Expression(code_string)
    assert expr.code_string == str(code_string)
    assert expr.evaluate(EvaluationContext()) == 1

    code_string = False
    expr = Expression(code_string)
    assert expr.code_string == str(code_string)
    assert expr.evaluate(EvaluationContext()) is False

    code_string = '"Hello World"'
    expr = Expression(code_string)
    assert expr.code_string == code_string
    assert expr.evaluate(EvaluationContext()) == "Hello World"

    code_string = '[1, 2, 3]'
    expr = Expression(code_string)
    assert expr.code_string == code_string
    assert expr.evaluate(EvaluationContext()) == [1, 2, 3]

    code_string = '{"a":1, "b":2}'
    expr = Expression(code_string)
    assert expr.code_string == code_string
    assert expr.evaluate(EvaluationContext()) == {"a": 1, "b": 2}


def test_expression_globals_locals() -> None:
    code_string = 'a + b + 1'
    expr = Expression(code_string)

    with pytest.raises(NameError, match='name \'a\' is not defined'):
        expr.evaluate(EvaluationContext())

    assert expr.evaluate(EvaluationContext(Context({'a': 2, 'b': 3}))) == 6
    assert expr.evaluate(EvaluationContext(local_context=Context({'a': 2, 'b': 3}))) == 6
    assert expr.evaluate(EvaluationContext(Context({'a': 2}), Context({'b': 3}))) == 6


def test_expression_conditional() -> None:
    code_string = '1 if 2 > 1 else 3'
    expr = Expression(code_string)
    assert expr.evaluate(EvaluationContext()) == 1


def test_expression_user_function() -> None:
    code_string = '2 if test_function() else 3'

    def test_function():
        return 3 > 4

    expr = Expression(code_string)
    assert expr.evaluate(EvaluationContext(Context({'test_function': test_function}))) == 3


def test_invalid_expression() -> None:
    code_string = '{9292#?&@&^'
    with raises(Exception):
        Expression(code_string)


def test_execution_error(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    code_string = '1/0'
    assert Expression(code_string).evaluate(EvaluationContext()) is None
    assert 'ZeroDivisionError in evaluating expression 1/0. Error: division by zero' in caplog.text


def test_execution_key_error(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    code_string = 'test_dict[\'missing_key\'] + 1'
    assert Expression(code_string).evaluate(EvaluationContext(Context({'test_dict': {}}))) is None
    assert 'KeyError in evaluating expression test_dict[\'missing_key\'] + 1. Error: \'missing_key\'' in caplog.records[
        0].message
    assert caplog.records[0].levelno == logging.DEBUG


def test_execution_error_type_mismatch(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    code_string = '1 + \'a\''
    assert Expression(code_string).evaluate(EvaluationContext(Context({'test_dict': {}}))) is None
    assert 'TypeError in evaluating expression 1 + \'a\'' in caplog.records[0].message
    assert caplog.records[0].levelno == logging.DEBUG


def test_execution_error_missing_aggregate(caplog, schema_loader: SchemaLoader) -> None:
    caplog.set_level(logging.DEBUG)
    context = Context({
        'test': StreamingTransformer(schema_loader.get_schema_object('test'), 'user1')
    })
    with raises(MissingAttributeError, match='missing_aggregate not defined in test'):
        Expression('test.missing_aggregate + 1').evaluate(EvaluationContext(context))
    assert ('MissingAttributeError in evaluating expression test.missing_aggregate + 1. '
            'Error: missing_aggregate not defined in test') in caplog.text

    with raises(MissingAttributeError, match='missing_aggregate not defined in test'):
        Expression('test[\'missing_aggregate\'] + 1').evaluate(EvaluationContext(context))


def test_execution_error_missing_field(caplog, schema_loader: SchemaLoader) -> None:
    caplog.set_level(logging.DEBUG)
    context = Context({
        'test': StreamingTransformer(schema_loader.get_schema_object('test'), 'user1')
    })
    with raises(MissingAttributeError, match='missing_field not defined in test_group'):
        Expression('test.test_group.missing_field').evaluate(EvaluationContext(context))
    assert ('MissingAttributeError in evaluating expression test.test_group.missing_field. '
            'Error: missing_field not defined in test_group') in caplog.text

    with raises(MissingAttributeError, match='missing_field not defined in test_group'):
        Expression('test.test_group[\'missing_field\']').evaluate(EvaluationContext(context))
