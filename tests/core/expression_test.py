from pytest import raises

from blurr.core.base import Expression
from blurr.core.errors import ExpressionEvaluationError, InvalidExpressionError
from blurr.core.evaluation import Context, EvaluationContext


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

    with raises(ExpressionEvaluationError, Message='is not defined'):
        expr.evaluate(EvaluationContext())

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


def test_expression_conditional() -> None:
    code_string = '1 if 2 > 1 else 3'
    expr = Expression(code_string)
    assert expr.evaluate(EvaluationContext()) == 1


def test_expression_user_function() -> None:
    code_string = '2 if test_function() else 3'

    def test_function():
        return 3 > 4

    expr = Expression(code_string)
    assert expr.evaluate(
        EvaluationContext(Context({
            'test_function': test_function
        }))) == 3


def test_invalid_expression() -> None:
    code_string = '{9292#?&@&^'
    with raises(InvalidExpressionError):
        Expression(code_string)


def test_execution_error() -> None:
    code_string = '1/0'
    with raises(ExpressionEvaluationError):
        Expression(code_string).evaluate(EvaluationContext())


def test_validate_valid() -> None:
    Expression('a==b')
    Expression('a == c')
    Expression('a==b == c')

    Expression('a!=b')
    Expression('a != c')
    Expression('a!=b != c')

    with raises(InvalidExpressionError, message='invalid syntax'):
        Expression('==a ==b== c==')

    with raises(InvalidExpressionError, message='invalid syntax'):
        Expression('!=a !=b!= c!=')

    with raises(InvalidExpressionError, message='invalid syntax'):
        Expression('!= ')

    with raises(InvalidExpressionError, message='invalid syntax'):
        Expression('!=b')

    with raises(InvalidExpressionError, message='invalid syntax'):
        Expression('c !=')

    with raises(InvalidExpressionError, message='invalid syntax'):
        Expression('== ')

    with raises(InvalidExpressionError, message='invalid syntax'):
        Expression('==b')

    with raises(InvalidExpressionError, message='invalid syntax'):
        Expression('c ==')


def test_validate_invalid() -> None:
    with raises(
            InvalidExpressionError,
            message='Setting value using `=` is not allowed.'):
        Expression('a= b')

    with raises(
            InvalidExpressionError,
            message='Setting value using `=` is not allowed.'):
        Expression('a!=b = ')

    with raises(
            InvalidExpressionError,
            message='Setting value using `=` is not allowed.'):
        Expression(' =a')

    with raises(
            InvalidExpressionError,
            message='Setting value using `=` is not allowed.'):
        Expression('b =')
