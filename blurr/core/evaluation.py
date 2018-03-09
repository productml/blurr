from typing import Any, Dict

from blurr.core.errors import ExpressionEvaluationException, InvalidExpressionException


class Context(dict):
    """
    Evaluation context provides a dictionary of declared context objects
    """

    def __init__(self, initial_value: Dict[str, Any] = None):
        """
        Initializes a new context with an existing dictionary
        :param initial_value: Dictionary mapping strings to execution context objects
        """
        if initial_value:
            super().__init__(initial_value)

    def add_context(self, name, value):
        self[name] = value

    def merge_context(self, context):
        self.update(context)


class Expression:
    """ Encapsulates a python code statement in string and in compilable expression"""

    def __init__(self, code_string: str) -> None:
        """
        An expression must be initialized with a python statement
        :param code_string: Python code statement
        """
        # TODO Add validation to see that there are no direct setting using the '=' character
        self.code_string = 'None' if code_string.isspace() else code_string

        try:
            self.code_object = compile(self.code_string, '<string>', 'eval')
        except Exception as e:
            raise InvalidExpressionException(e)

    def evaluate(self, global_context: Context = Context(), local_context: Context = Context()) -> Any:
        """
        Evaluates the expression with the context provided.  If the execution
        results in failure, an ExpressionEvaluationException encapsulating the
        underlying exception is raised.
        :param global_context: Global context dictionary to be passed for evaluation
        :param local_context: Local Context dictionary to be passed for evaluation
        """
        try:
            return eval(self.code_object, global_context, local_context)

        except Exception as e:
            raise ExpressionEvaluationException(e)
