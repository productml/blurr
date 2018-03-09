from abc import ABC, abstractmethod
from typing import Dict, Any

from blurr.core.context import Context
from blurr.core.errors import InvalidSchemaException, ExpressionEvaluationException


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


class BaseSchema(ABC):
    """
    The Base Schema encapsulates the common functionality of all schema
    elements
    """

    # Field Name Definitions
    FIELD_NAME = 'Name'
    FIELD_TYPE = 'Type'
    FIELD_WHEN = 'When'

    def __init__(self, spec: Dict[str, Any]) -> None:
        """
        A schema object must be initialized with a schema spec
        :param spec: Dictionary representation of the YAML schema spec
        """
        # Load the schema spec into the current object

        self.__validate_spec(spec)
        self.__load_spec(spec)

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

    def __load_spec(self, spec: Dict[str, Any]) -> None:
        """
        Loads the base schema spec into the object
        """
        self.spec: Dict[str, Any] = spec
        self.name: str = spec[self.FIELD_NAME]
        self.type: str = spec[self.FIELD_TYPE]
        self.when: Expression = Expression(spec[self.FIELD_WHEN])

        # Invokes the loads of the subclass
        self.load(spec)

    def __validate_spec(self, spec: Dict[str, Any]) -> None:
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
    """
    Base class for for all items
    """

    def __init__(self, schema: BaseSchema, global_context: Context = Context(), local_context: Context = Context()):
        """
        Initializes an item with the schema and execution context
        :param schema: Schema of the item
        :param global_context: Global context dictionary for evaluation
        :param local_context: Local context dictionary for evaluation
        """
        self.schema = schema
        self.global_context = global_context
        self.local_context = local_context

    def evaluate(self) -> EvaluationResult:
        """
        Evaluates the current item
        :returns An evaluation result object containing the result, or reasons why
        evaluation failed
        """
        when_result = self.schema.when.evaluate(self.global_context, self.local_context)
        if when_result.success and when_result.result:
            return self.evaluate_item()

        return EvaluationResult(None,
                                'When condition evaluated to False' if when_result.success else 'Error occurred')

    @abstractmethod
    def evaluate_item(self) -> EvaluationResult:
        """
        Implements specific behavior for evaluation
        """
        raise NotImplementedError('evaluate_item must be implemented')


class BaseType(ABC):
    """
    Base for all field types supported
    """

    @property
    @abstractmethod
    def type_object(self) -> Any:
        """
        Returns the type object the Type represents
        """
        raise NotImplementedError('type_object is required')

    def is_type_of(self, instance: Any) -> bool:
        """
        Checks if instance is of the type
        :param instance: An object instance
        :return: True if the object is of this type, False otherwise
        """
        return isinstance(instance, self.type_object)

    @property
    @abstractmethod
    def default(self) -> Any:
        """
        Returns the default value for this type
        """
        raise NotImplementedError('type_object is required')

    def diff(self, old: Any, new: Any) -> Any:
        """
        Returns the difference between two objects of the type
        :param old:
        :param new:
        :return:
        """
        if not self.is_type_of(old) or not self.is_type_of(new):
            raise TypeError('old and new are not objects of this type')

        return self.calculate_difference(old, new)

    @abstractmethod
    def calculate_difference(self, old: Any, new: Any) -> Any:
        raise NotImplementedError('type_object is required')
