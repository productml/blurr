from copy import copy
from enum import Enum
from typing import Any, Dict

from blurr.core import logging
from blurr.core.errors import MissingAttributeError
from blurr.core.record import Record


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
    def __init__(self, global_context: Context = None, local_context: Context = None) -> None:
        """
        Initializes an evaluation context with global and local context dictionaries.  The unset parameters default
        to an empty context dictionary.
        :param global_context: Global context dictionary
        :param local_context: Local context dictionary.
        """
        self.global_context = Context() if global_context is None else global_context
        self.local_context = Context() if local_context is None else local_context

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

    def global_add(self, key: str, value: Any) -> None:
        """
        Adds a key and value to the global dictionary
        """
        self.global_context[key] = value

    def global_remove(self, key: str) -> None:
        """
        Removes the key and its value from the global dictionary
        """
        del self.global_context[key]

    def add_record(self, record: Record) -> None:
        """
        Adds a record to the evaluation context.
        :param record: Record to be added
        """
        self.global_add('source', record)

    def remove_record(self) -> None:
        """Removes any previously added record to the evaluation context."""
        del self.global_context['source']

    def merge(self, evaluation_context: 'EvaluationContext') -> None:
        """
        Merges the provided evaluation context to the current evaluation context.
        :param evaluation_context: Evaluation context to merge.
        """
        self.global_context.merge(evaluation_context.global_context)
        self.local_context.merge(evaluation_context.local_context)


class ExpressionType(Enum):
    EVAL = 'eval'
    EXEC = 'exec'


class Expression:
    """ Encapsulates a python code statement in string and in compilable expression"""

    def __init__(self, code_string: str,
                 expression_type: ExpressionType = ExpressionType.EVAL) -> None:
        """
        An expression must be initialized with a python statement
        :param code_string: Python code statement
        """

        self.code_string = str(code_string)
        self.type = expression_type

        self.code_object = compile(self.code_string, '<string>', self.type.value)

    def evaluate(self, evaluation_context: EvaluationContext) -> Any:
        """
        Evaluates the expression with the context provided.  If the execution
        results in failure, an ExpressionEvaluationException encapsulating the
        underlying exception is raised.
        :param evaluation_context: Global and local context dictionary to be passed for evaluation
        """
        try:

            if self.type == ExpressionType.EVAL:
                return eval(self.code_object, evaluation_context.global_context,
                            evaluation_context.local_context)

            elif self.type == ExpressionType.EXEC:
                return exec(self.code_object, evaluation_context.global_context,
                            evaluation_context.local_context)

        except Exception as err:
            # Evaluation exceptions are expected because of missing fields in the source 'Record'.
            logging.debug('{} in evaluating expression {}. Error: {}'.format(
                type(err).__name__, self.code_string, err))
            # These should result in an exception being raised:
            # NameError - Exceptions thrown because of using names in the expression which are not
            #   present in EvaluationContext. A common cause for this is typos in the DTC.
            # MissingAttributeError - Exception thrown when a DTC nested item is used which does not
            #   exist. Should only happen for erroneous DTCs.
            # ImportError - Thrown when there is a failure in importing other modules.
            if isinstance(err, (NameError, MissingAttributeError, ImportError)):
                raise err
            return None
