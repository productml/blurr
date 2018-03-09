from abc import ABC, abstractmethod
from typing import Dict, Any

from blurr.core.evaluation import Context, Expression
from blurr.core.errors import InvalidSchemaException


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
        self.when: Expression = Expression(spec[self.FIELD_WHEN]) if self.FIELD_WHEN in spec else None

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
    Base class for for all leaf items that do not contain sub-items
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

    @property
    def needs_evaluation(self) -> bool:
        return bool(self.schema.when and self.schema.when.evaluate())

    @property
    def name(self):
        return self.schema.name

    @abstractmethod
    def evaluate(self) -> None:
        """
        Evaluates the current item
        """
        raise NotImplementedError('evaluate() must be implemented')

    @property
    @abstractmethod
    def export(self):
        raise NotImplementedError('export() must be implemented')


class BaseItemCollection(BaseItem):
    """
    Base class for items that contain sub-items within them
    """

    def evaluate(self) -> None:
        """
        Evaluates the current item
        :returns An evaluation result object containing the result, or reasons why
        evaluation failed
        """
        if self.needs_evaluation:
            for _, item in self.items.items():
                item.evaluate()

    @property
    def export(self):
        return {
            name: item.export() for name, item in self.items.items()
        }

    @property
    @abstractmethod
    def items(self):
        raise NotImplementedError('Sub items must be present')
