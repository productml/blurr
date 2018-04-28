from abc import ABC, abstractmethod
from typing import Dict, Any, Type, TypeVar, Union, List, Optional

from blurr.core.errors import SnapshotError, SchemaErrorCollection, InvalidSchemaError
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.validator import validate_required, validate_identifier, validate_number


class BaseSchema(ABC):
    """ Encapsulates the common functionality of all schema elements """

    ATTRIBUTE_NAME = 'Name'
    ATTRIBUTE_DESCRIPTION = 'Description'
    ATTRIBUTE_TYPE = 'Type'
    ATTRIBUTE_WHEN = 'When'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        """
        Initializes a schema by providing the path to the schema and the schema loader resource
        :param fully_qualified_name: Fully qualified path to the schema
        :param schema_loader: Schema repository that returns schema spec by fully qualified name
        """
        self.schema_loader: SchemaLoader = schema_loader
        self.fully_qualified_name: str = fully_qualified_name
        self._spec: Dict[str, Any] = self.schema_loader.get_schema_spec(self.fully_qualified_name)

        self._errors = self.validate_schema_spec()

        self._spec: Dict[str, Any] = self.extend_schema_spec(self._spec)

        self.name: str = self._spec[self.ATTRIBUTE_NAME]
        self.type: str = self._spec[self.ATTRIBUTE_TYPE]

        self.when: Expression = Expression(
            self._spec[self.ATTRIBUTE_WHEN]) if self.ATTRIBUTE_WHEN in self._spec else None
        self.description: str = self._spec.get(self.ATTRIBUTE_DESCRIPTION, None)

    def extend_schema_spec(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """ Extends the defined schema specifications at runtime with defaults """
        return spec

    def add_errors(self, *errors: Union[InvalidSchemaError, SchemaErrorCollection]) -> None:
        """ Adds errors to the error repository in schema laoder """
        self.schema_loader.add_errors(*errors)

    @property
    def errors(self) -> List[InvalidSchemaError]:
        """ Returns a list of errors raised by this schema """
        return self.schema_loader.get_errors(self.fully_qualified_name)

    def validate_required(self, *attributes) -> None:
        """ Validates that the schema contains a series of required attributes """
        self.add_errors(validate_required(self.fully_qualified_name, self._spec, *attributes))

    def validate_identity(self, *attributes) -> None:
        """ Validates that a schema attribute can be a python valid identifier """
        self.add_errors(validate_identifier(self.fully_qualified_name, self._spec, *attributes))

    def validate_number(self, attribute: str, value_type: Union[Type[int], Type[float]] = int,
                        minimum: Optional[Union[int, float]] = None, maximum: Optional[Union[int, float]] = None):
        self.add_errors(
            validate_number(self.fully_qualified_name, self._spec, attribute, value_type, minimum, maximum))

    @abstractmethod
    def validate_schema_spec(self) -> None:
        """ Contains the validation routines that are to be executed as part of initialization by subclasses"""
        raise NotImplementedError('Schema spec validation must be implemented.')


class BaseSchemaCollection(BaseSchema, ABC):
    """
    Base class for schema that contain nested schema
    """

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader,
                 nested_schema_attribute: str) -> None:
        """
        Initializes a schema collection with nested items
        :param fully_qualified_name: Fully qualified path to the schema
        :param schema_loader: Schema repository that returns schema spec by fully qualified name
        :param nested_schema_attribute: Name of the attribute that contains the nested elements
        """
        self._nested_item_attribute = nested_schema_attribute

        super().__init__(fully_qualified_name, schema_loader)

        # Load nested schema items
        self.nested_schema: Dict[str, Type[BaseSchema]] = {
            schema_spec[self.ATTRIBUTE_NAME]: self.schema_loader.get_nested_schema_object(
                self.fully_qualified_name, schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec.get(self._nested_item_attribute, [])
        }

    def validate_schema_spec(self) -> None:
        self.validate_required(self._nested_item_attribute)


BaseItemType = TypeVar('T', bound='BaseItem')


class BaseItem(ABC):
    """
    Base class for for all leaf items that do not contain sub-items
    """

    def __init__(self, schema: BaseSchema, evaluation_context: EvaluationContext) -> None:
        """
        Initializes an item with the schema and execution context
        :param schema: Schema of the item
        :param evaluation_context: Context dictionary for evaluation
        """
        self._schema = schema
        self._evaluation_context: EvaluationContext = evaluation_context

    @property
    def _needs_evaluation(self) -> bool:
        """
        Returns True when:
            1. Where clause is not specified
            2. Where WHERE clause is specified and it evaluates to True
        Returns false if a where clause is specified and it evaluates to False
        """
        return self._schema.when is None or self._schema.when.evaluate(self._evaluation_context)

    @property
    def _name(self) -> str:
        """
        Returns the name of the base item
        """
        return self._schema.name

    @abstractmethod
    def evaluate(self, *args, **kwargs) -> None:
        """
        Evaluates the current item
        """
        raise NotImplementedError('evaluate() must be implemented')

    @property
    @abstractmethod
    def _snapshot(self):
        """
        Gets a dictionary representation of the current state items in the current hierarchy
        :return: Name, Value map of the current tree
        """
        raise NotImplementedError('snapshot() must be implemented')

    @abstractmethod
    def restore(self, snapshot) -> BaseItemType:
        """
        Restores the state of an item from a snapshot
        """
        raise NotImplementedError('restore() must be implemented')

    @abstractmethod
    def reset(self) -> None:
        """
        Resets the state of the item.
        """
        raise NotImplementedError('reset() must be implemented')


class BaseItemCollection(BaseItem, ABC):
    """
    Base class for items that contain sub-items within them
    """

    def __init__(self, schema: BaseSchemaCollection, evaluation_context: EvaluationContext) -> None:
        """
        Loads nested items to the 'items' collection
        :param schema: Schema that conforms to the item
        :param evaluation_context: Context dictionary for evaluation
        """

        super().__init__(schema, evaluation_context)

    def evaluate(self, *args, **kwargs) -> None:
        """
        Evaluates the current item
        :returns An evaluation result object containing the result, or reasons why
        evaluation failed
        """
        if self._needs_evaluation:
            for _, item in self._nested_items.items():
                item.evaluate()

    @property
    def _snapshot(self) -> Dict[str, Any]:
        """
        Implements snapshot for collections by recursively invoking snapshot of all child items
        """
        try:
            return {name: item._snapshot for name, item in self._nested_items.items()}
        except Exception as e:
            raise SnapshotError('Error while creating snapshot for {}'.format(self._name)) from e

    def restore(self, snapshot: Dict[Union[str, Key], Any]) -> 'BaseItemCollection':
        """
        Restores the state of a collection from a snapshot
        """
        try:

            for name, snap in snapshot.items():
                if isinstance(name, Key):
                    self._nested_items[name.group].restore(snap)
                else:
                    self._nested_items[name].restore(snap)
            return self

        except Exception as e:
            raise SnapshotError('Error while restoring snapshot: {}'.format(self._snapshot)) from e

    def reset(self) -> None:
        for _, item in self._nested_items.items():
            item.reset()

    @abstractmethod
    def finalize(self) -> None:
        """
        Performs the final rites of an item before it decommissioned
        """
        raise NotImplementedError('finalize() must be implemented')

    @property
    @abstractmethod
    def _nested_items(self) -> Dict[str, Type[BaseItem]]:
        """
        Dictionary of the name and item in the collection
        """
        raise NotImplementedError('nested_items() must be implemented')
