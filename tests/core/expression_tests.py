from pytest import raises

from blurr.core.base import Expression
from blurr.core.errors import ExpressionEvaluationError, InvalidExpressionError
from blurr.core.evaluation import Context, EvaluationContext


def test_expression_valid() -> None:
    code_string = '1+1'
    expr = Expression(code_string)
    assert expr.code_string == code_string
    assert expr.evaluate(None, None) == 2


def test_expression_globals_locals() -> None:
    code_string = 'a + b + 1'
    expr = Expression(code_string)

    with raises(ExpressionEvaluationError, Message='is not defined'):
        expr.evaluate()

    assert expr.evaluate(EvaluationContext(Context({'a': 2, 'b': 3}))) == 6
    assert expr.evaluate(
        EvaluationContext(local_context=Context({
            'a': 2,
            'b': 3
        }))) == 6
    assert expr.evaluate(
        EvaluationContext(Context({
            'a': 2
        }), Context({
            'b': 3
        }))) == 6


def test_invalid_expression() -> None:
    code_string = '{9292#?&@&^'
    with raises(InvalidExpressionError):
        Expression(code_string)


def test_execution_error() -> None:
    code_string = '1/0'
    with raises(ExpressionEvaluationError):
        Expression(code_string).evaluate()
