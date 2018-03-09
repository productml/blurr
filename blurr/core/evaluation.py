from typing import Any
from blurr.core.errors import ExpressionEvaluationException


class Context(dict):
    """
    Evaluation context provides a dictionary of declared context objects
    """
    def add_context(self, name, value):
        self[name] = value

    def merge_context(self, context):
        self.update(context)


class EvaluationResult:
    """
    Returned as the result of an evaluation
    """

    def __init__(self, result: Any = None, skip_cause: str = None, error: Exception = None) -> None:
        """
        Initializes an evaluation result withe the result and result behavior
        :param result: Result of the evaluation
        :param skip_cause: Reason for not evaluating
        :param error: Exception thrown during execution
        """
        self.result = result
        self.success = not skip_cause and not error
        self.skip_cause = skip_cause
        self.error = error


class Expression:
    """ Encapsulates a python code statement in string and in compilable expression"""

    def __init__(self, code_string: str) -> None:
        """
        An expression must be initialized with a python statement
        :param code_string: Python code statement
        """
        self.code_string = 'None' if code_string.isspace() else code_string
        self.code_object = compile(self.code_string, '<string>', 'eval')

    def evaluate(self, global_context: Context = Context(), local_context: Context = Context()) -> EvaluationResult:
        """
        Evaluates the expression with the context provided.  If the execution
        results in failure, an ExpressionEvaluationException encapsulating the
        underlying exception is raised.
        :param global_context: Global context dictionary to be passed for evaluation
        :param local_context: Local Context dictionary to be passed for evaluation
        """
        try:
            return EvaluationResult(eval(self.code_object, global_context, local_context))

        except Exception as e:
            EvaluationResult(error=ExpressionEvaluationException(e))
