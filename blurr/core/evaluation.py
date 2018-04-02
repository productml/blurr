from typing import Any, Dict

import re
from copy import copy

from blurr.core.errors import ExpressionEvaluationError, InvalidExpressionError


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

    def add(self, name, value):
        """
        Adds a context item by name
        :param name: Name of the item in the context for evaluation
        :param value: Object that the name refers to in the context
        """
        self[name] = value

    def merge(self, context: 'Context') -> None:
        """
        Updates the context object with the items in another context
        object.  Fields of the same name are overwritten
        :param context: Another context object to superimpose on current
        :return:
        """
        self.update(context)


class EvaluationContext:
    def __init__(self,
                 global_context: Context = None,
                 local_context: Context = None) -> None:
        """
        Initializes an evaluation context with global and local context dictionaries.  The unset parameters default
        to an empty context dictionary.
        :param global_context: Global context dictionary
        :param local_context: Local context dictionary.
        """
        self.global_context = Context(
        ) if global_context is None else global_context
        self.local_context = Context(
        ) if local_context is None else local_context

    @property
    def fork(self) -> 'EvaluationContext':
        """
        Creates a copy of the current evaluation config with the same global context, but a shallow copy of the
        local context
        :return:
        """
        return EvaluationContext(self.global_context, copy(self.local_context))

    def local_include(self, context: Context) -> None:
        """
        Includes the supplied context into the local context
        :param context: Context to include into local context
        """
        self.local_context.merge(context)

    def global_add(self, key: str, value: Any) -> Any:
        """
        Adds a key and value to the global dictionary
        """
        self.global_context[key] = value


VALIDATION_INVALID_EQUALS_REGULAR_EXPRESSION = re.compile(
    '(?:^|[^!=]+)=(?:[^=]+|$)')


class Expression:
    """ Encapsulates a python code statement in string and in compilable expression"""

    def __init__(self, code_string: str) -> None:
        """
        An expression must be initialized with a python statement
        :param code_string: Python code statement
        """

        # For non string single value expressions. Ex: 5, False.
        code_string = str(code_string)

        # For None / empty code strings
        self.code_string = 'None' if code_string.isspace() else code_string

        # Validate the expression for errors / unsupported expressions
        if VALIDATION_INVALID_EQUALS_REGULAR_EXPRESSION.findall(code_string):
            raise InvalidExpressionError(
                'Modifying value using `=` is not allowed.')

        try:
            self.code_object = compile(self.code_string, '<string>', 'eval')
        except Exception as e:
            raise InvalidExpressionError(e)

    def evaluate(self, evaluation_context: EvaluationContext) -> Any:
        """
        Evaluates the expression with the context provided.  If the execution
        results in failure, an ExpressionEvaluationException encapsulating the
        underlying exception is raised.
        :param evaluation_context: Global and local context dictionary to be passed for evaluation
        """
        try:
            return eval(self.code_object, evaluation_context.global_context,
                        evaluation_context.local_context)
        except:
            # TODO Log exception
            return None
