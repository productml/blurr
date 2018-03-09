from abc import ABC, abstractmethod
from blurr.core.interpreter import Interpreter
from typing import Dict, Any
from blurr.core.errors import InvalidSchemaException


class Expression:
    """ Encapsulates a python code statement in string and in compilable expression"""

    def __init__(self, code_string):
        self.code_string = 'None' if code_string.isspace() else code_string
        self.code_object = compile(self.code_string, '<string>', 'eval')

    def evaluate(self, global_dictionary, local_dictionary):
        eval(self.code_object, global_dictionary, local_dictionary)


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
    def __init__(self, schema: BaseSchema):
        self._schema = schema

    def should_evaluate(self, interpreter: Interpreter) -> None:
        return not self._schema.filter_expr or interpreter.evaluate(
            self._schema.filter_expr)

    @abstractmethod
    def evaluate(self, interpreter: Interpreter) -> None:
        return NotImplemented
