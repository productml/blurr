from typing import Dict, Any, Type

from abc import ABC, abstractmethod

from blurr.core.errors import SnapshotError
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader


class BaseSchema(ABC):
    """
    The Base Schema encapsulates the common functionality of all schema
    elements
    """

    # Field Name Definitions
    ATTRIBUTE_NAME = 'Name'
    ATTRIBUTE_TYPE = 'Type'
    ATTRIBUTE_WHEN = 'When'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        """
        A schema object must be initialized with a schema spec
        :param spec: Dictionary representation of the YAML schema spec
        """
        self.schema_loader = schema_loader
        self.fully_qualified_name = fully_qualified_name
        self.__load_spec()

    @abstractmethod
    def validate(self, spec: Dict[str, Any]) -> None:
        """
        Abstract method that ensures that the subclasses implement spec validation
        """
        raise NotImplementedError(
            '"validate()" must be implemented for a schema.')

    @abstractmethod
    def load(self) -> None:
        """
        Abstract method placeholder for subclasses to load the spec into the schema
        """
        raise NotImplementedError('"load()" must be implemented for a schema.')

    def __load_spec(self) -> None:
        """
        Loads the base schema spec into the object
        """
        self._spec: Dict[str, Any] = self.schema_loader.get_schema_spec(
            self.fully_qualified_name)
        self.name: str = self._spec[self.ATTRIBUTE_NAME]
        self.type: str = self._spec[self.ATTRIBUTE_TYPE]
        self.when: Expression = Expression(
            self._spec[self.ATTRIBUTE_WHEN]
        ) if self.ATTRIBUTE_WHEN in self._spec else None

        # Invokes the loads of the subclass
        self.load()


class BaseSchemaCollection(BaseSchema, ABC):
    """
    Base class for schema that contain nested schema
    """

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader,
                 nested_schema_attribute: str) -> None:
        """
        Initializes the schema for schema that contain a nested schema
        :param spec:
        :param nested_schema_attribute:
        """
        # Must declare all new fields prior to the initialization so that validation can find the new fields
        self._nested_item_attribute = nested_schema_attribute

        super().__init__(fully_qualified_name, schema_loader)

    def validate(self, spec: Dict[str, Any]):
        """
        Overrides the Base Schema validation specifications to include validation for nested schema
        """

        # Validate that the nested schema attribute is present
        self.validate_required_attribute(spec, self._nested_item_attribute)

        # Ensure that the nested schema attribute is a list
        if not isinstance(spec[self._nested_item_attribute], list):
            self.raise_validation_error(
                spec, self._nested_item_attribute,
                'Schema definition must specify a list of {}.'.format(
                    self._nested_item_attribute))

        # Ensure that the nested schema attribute contains a list of one or more items
        if len(spec[self._nested_item_attribute]) == 0:
            self.raise_validation_error(
                spec, self._nested_item_attribute,
                'Schema definition must have at least one item under {}.'.
                format(self._nested_item_attribute))

    def load(self) -> None:
        """
        Overrides base load to include loads for nested items
        """

        # Load nested schema items
        self.nested_schema: Dict[str, Type[BaseSchema]] = {
            schema_spec[self.ATTRIBUTE_NAME]:
            self.schema_loader.get_schema_object(
                self.fully_qualified_name + '.' +
                schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec[self._nested_item_attribute]
        }


class BaseItem(ABC):
    """
    Base class for for all leaf items that do not contain sub-items
    """

    def __init__(self, schema: BaseSchema,
                 evaluation_context: EvaluationContext) -> None:
        """
        Initializes an item with the schema and execution context
        :param schema: Schema of the item
        :param evaluation_context: Context dictionary for evaluation
        """
        self.schema = schema
        self.evaluation_context = evaluation_context

    @property
    def needs_evaluation(self) -> bool:
        """
        Returns True when:
            1. Where clause is not specified
            2. Where WHERE clause is specified and it evaluates to True
        Returns false if a where clause is specified and it evaluates to False
        """
        return self.schema.when is None or self.schema.when.evaluate(
            self.evaluation_context)

    @property
    def name(self) -> str:
        """
        Returns the name of the base item
        """
        return self.schema.name

    @abstractmethod
    def evaluate(self) -> None:
        """
        Evaluates the current item
        """
        raise NotImplementedError('evaluate() must be implemented')

    @property
    @abstractmethod
    def snapshot(self):
        """
        Gets a dictionary representation of the current state items in the current hierarchy
        :return: Name, Value map of the current tree
        """
        raise NotImplementedError('snapshot() must be implemented')

    @abstractmethod
    def restore(self, snapshot) -> 'BaseItem':
        """
        Restores the state of an item from a snapshot
        """
        raise NotImplementedError('restore() must be implemented')


class BaseItemCollection(BaseItem):
    """
    Base class for items that contain sub-items within them
    """

    def __init__(self, schema: BaseSchemaCollection,
                 evaluation_context: EvaluationContext) -> None:
        """
        Loads nested items to the 'items' collection
        :param schema: Schema that conforms to the item
        :param evaluation_context: Context dictionary for evaluation
        """

        super().__init__(schema, evaluation_context)

        # Load the nested items into the item
        self.nested_items: Dict[str, Type[BaseItem]] = {
            name: TypeLoader.load_item(item_schema.type)(
                item_schema, self.evaluation_context)
            for name, item_schema in self.schema.nested_schema.items()
        }

    def evaluate(self) -> None:
        """
        Evaluates the current item
        :returns An evaluation result object containing the result, or reasons why
        evaluation failed
        """
        if self.needs_evaluation:
            for _, item in self.nested_items.items():
                item.evaluate()

    @property
    def snapshot(self) -> Dict[str, Any]:
        """
        Implements snapshot for collections by recursively invoking snapshot of all child items
        """
        try:

            return {
                name: item.snapshot
                for name, item in self.nested_items.items()
            }

        except Exception as e:
            print('Error while creating snapshot for {}', self.name)
            raise SnapshotError(e)

    def restore(self, snapshot: Dict[str, Any]) -> BaseItem:
        """
        Restores the state of a collection from a snapshot
        """
        try:

            for name, snap in snapshot.items():
                self.nested_items[name].restore(snap)
            return self

        except Exception as e:
            print('Error while restoring snapshot: {}', self.snapshot)
            raise SnapshotError(e)

    def __getattr__(self, item: str) -> Any:
        """
        Makes the value of the nested items available as properties
        of the collection object.  This is used for retrieving field values
        for dynamic execution.
        :param item: Field requested
        """
        if item in self.nested_items:
            return self.nested_items[item].snapshot

        return self.__getattribute__(item)
