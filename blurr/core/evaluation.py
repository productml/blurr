from typing import Any, Dict

from blurr.core.errors import ExpressionEvaluationException, InvalidExpressionException


class EvaluationContext(dict):
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

    def add(self, name, value):
        """
        Adds a context item by name
        :param name: Name of the item in the context for evaluation
        :param value: Object that the name refers to in the context
        """
        self[name] = value

    def merge(self, context: 'EvaluationContext') -> None:
        """
        Updates the context object with the items in another context
        object.  Fields of the same name are overwritten
        :param context: Another context object to superimpose on current
        :return:
        """
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

    def evaluate(self,
                 global_context: EvaluationContext = EvaluationContext(),
                 local_context: EvaluationContext = EvaluationContext()) -> Any:
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
