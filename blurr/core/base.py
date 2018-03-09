from abc import ABC, abstractmethod
from blurr.core.context import Context
import dis

from typing import Dict, Any
from blurr.core.errors import InvalidSchemaException, ExpressionEvaluationException


class Expression:
    """ Encapsulates a python code statement in string and in compilable expression"""

    def __init__(self, code_string: str) -> None:
        """
        An expression must be initialized with a python statement
        :param code_string: Python code statement
        """
        self.code_string = 'None' if code_string.isspace() else code_string
        self.code_object = compile(self.code_string, '<string>', 'eval')

    def evaluate(self, global_context: Dict[str, Any]=None, local_context: Dict[str, Any]=None) -> Any:
        """
        Evaluates the expression with the context provided.  If the execution
        results in failure, an ExpressionEvaluationException encapsulating the
        underlying exception is raised.
        :param global_context: Global context dictionary to be passed for evaluation
        :param local_context: Local Context dictionary to be passed for evaluation
        """
        try:
            return eval(self.code_object,
                        global_context if global_context else {},
                        local_context if local_context else {})
        except Exception as e:
            raise ExpressionEvaluationException(e)


class BaseSchema(ABC):
    """
    The Base Schema encapsulates the common functionality of all schema
    elements
    """

    # Field Name Definitions
    FIELD_NAME = 'Name'
    FIELD_TYPE = 'Type'
    FIELD_FILTER = 'Filter'

    def __init__(self, spec: Dict[str, Any]) -> None:
        """
        A schema object must be initialized with a schema spec
        :param spec: Dictionary representation of the YAML schema spec
        """
        # Load the schema spec into the current object

        self.validate_spec(spec)
        self.load_spec(spec)

    @abstractmethod
    def validate(self, spec: Dict[str, Any]) -> None:
        """
        Abstract method that ensures that the subclasses implement spec validation
        """
        raise NotImplementedError('"validate()" must be implemented for a schema.')

    @abstractmethod
    def load(self, spec: Dict[str, Any]) -> None:
        """
        Abstract method placeholder for subclasses to load the spec into the schema
        """
        raise NotImplementedError('"load()" must be implemented for a schema.')

    def load_spec(self, spec: Dict[str, Any]) -> None:
        """
        Loads the base schema spec into the object
        """
        self.spec: Dict[str, Any] = spec
        self.name: str = spec[self.FIELD_NAME]
        self.type: str = spec[self.FIELD_TYPE]
        self.filter: Expression = Expression(spec[self.FIELD_FILTER])

        # Invokes the loads of the subclass
        self.load(spec)

    def validate_spec(self, spec: Dict[str, Any]) -> None:
        """
        Validates the schema spec.  Raises exceptions if errors are found.
        """
        self.validate_required_attribute(spec, self.FIELD_NAME)
        self.validate_required_attribute(spec, self.FIELD_TYPE)

        # Invokes the validations of the subclasses
        self.validate(spec)

    def validate_required_attribute(self, spec: Dict[str, Any], attribute: str):
        """
        Raises an error if a required attribute is not defined
        or contains an empty value
        :param spec: Schema specifications
        :param attribute: Attribute that is being validated
        """
        if attribute not in spec:
            self.raise_validation_error(spec, attribute, 'Required attribute missing.')

        if isinstance(spec[attribute], str) and spec[attribute].isspace():
            self.raise_validation_error(spec, attribute, 'Invalid attribute value.')

    def raise_validation_error(self, spec: Dict[str, Any], attribute: str, message: str):
        """
        Raises an InvalidSchemaException exception with an expressive message
        :param spec: Schema specification dictionary
        :param attribute: Attribute with error
        :param message: Description of error encountered
        """
        error_message = ('\nError processing schema spec:'
                         '\n\tSpec: {name}'
                         '\n\tAttribute: {attribute}'
                         '\n\tError Message: {message}') \
            .format(
            name=spec.get(self.FIELD_NAME, str(spec)),
            attribute=attribute,
            message=message)
        raise InvalidSchemaException(error_message)


class BaseItem(ABC):
    def __init__(self, schema: BaseSchema, global_context: Context=Context(), local_context: Context=Context()):
        self._schema = schema
        self._global_context = global_context
        self._local_context = local_context

    def evaluate_expr(self, expr):
        try:
            return eval(expr, self._global_context, self._local_context)
        except Exception as e:
            print(dis.dis(expr))
            raise e

    def should_evaluate(self) -> None:
        return self._schema.filter.evaluate(self._global_context, self._local_context)

    @abstractmethod
    def evaluate(self) -> None:
        return NotImplemented
