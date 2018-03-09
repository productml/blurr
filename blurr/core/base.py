from abc import ABC, abstractmethod
from blurr.core.context import Context
import dis

from typing import Dict, Any
from blurr.core.errors import InvalidSchemaException


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

        # self.validate_spec(spec)
        self.load_spec(spec)

    def validate(self, spec: Dict[str, Any]) -> None:
        """
        Abstract method that ensures that the subclasses implement spec validation
        """
        raise NotImplementedError(
            '"validate()" must be implemented for a schema.')

    def validate_spec(self, spec: Dict[str, Any]) -> None:
        """
        Validates the schema spec.  Raises exceptions if errors are found.
        """
        if self.FIELD_NAME not in spec:
            self.raise_validation_error(spec, self.FIELD_NAME,
                                        'Required attribute missing.')

        if self.FIELD_TYPE not in spec:
            self.raise_validation_error(spec, self.FIELD_TYPE,
                                        'Required attribute missing.')

        # Invokes the validations of the subclasses
        self.validate(spec)

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
        self.filter: str = spec.get(self.FIELD_FILTER, None)
        self.filter_expr = None if self.filter is None else compile(
            self.filter, '<string>', 'eval')

        # Invokes the loads of the subclass
        # self.load(spec)

    def raise_validation_error(self, spec: Dict[str, Any], attribute: str,
                               message: str):
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
    def __init__(self,
                 schema: BaseSchema,
                 global_context: Context = Context(),
                 local_context: Context = Context()):
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
        return not self._schema.filter_expr or self.evaluate_expr(
            self._schema.filter_expr)

    def evaluate(self) -> None:
        if self.should_evaluate():
            for _, item in self.sub_items.items():
                item.evaluate()

    def build_json(self):
        return {
            name: item.build_json() for name, item in self.sub_items.items()
        }

    @property
    @abstractmethod
    def sub_items(self):
        return NotImplemented



