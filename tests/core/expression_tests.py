from pytest import raises

from blurr.core.base import Expression
from blurr.core.errors import ExpressionEvaluationException


def test_expression_valid() -> None:
    code_string = '1+1'
    expr = Expression(code_string)
    assert expr.code_string == code_string
    assert expr.evaluate(None, None) == 2


def test_expression_globals_locals() -> None:
    code_string = 'a + b + 1'
    expr = Expression(code_string)

    with raises(ExpressionEvaluationException, Message='is not defined'):
        expr.evaluate()

    assert expr.evaluate(global_context={'a': 2, 'b': 3}) == 6
    assert expr.evaluate(local_context={'a': 2, 'b': 3}) == 6
    assert expr.evaluate({'a': 2}, {'b': 3}) == 6
