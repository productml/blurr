from typing import Dict, Any, Type, TypeVar

from abc import ABC, abstractmethod

from blurr.core.errors import SnapshotError
from blurr.core.evaluation import Expression, EvaluationContext
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
        self._spec: Dict[str, Any] = self.schema_loader.get_schema_spec(
            self.fully_qualified_name)

        self.extend_schema()

        self.name: str = self._spec[self.ATTRIBUTE_NAME]
        self.type: str = self._spec[self.ATTRIBUTE_TYPE]
        self.when: Expression = Expression(
            self._spec[self.ATTRIBUTE_WHEN]
        ) if self.ATTRIBUTE_WHEN in self._spec else None

    def extend_schema(self):
        pass


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
        super().__init__(fully_qualified_name, schema_loader)

        self._nested_item_attribute = nested_schema_attribute
        # Load nested schema items
        self.nested_schema: Dict[str, Type[BaseSchema]] = {
            schema_spec[self.ATTRIBUTE_NAME]:
            self.schema_loader.get_nested_schema_object(
                self.fully_qualified_name, schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec[self._nested_item_attribute]
        }


BaseItemType = TypeVar('T', bound='BaseItem')


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
    def restore(self, snapshot) -> BaseItemType:
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

    def restore(self, snapshot: Dict[str, Any]) -> 'BaseItemCollection':
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

    @abstractmethod
    def finalize(self) -> None:
        """
        Performs the final rites of an item before it decommissioned
        """
        raise NotImplementedError('finalize() must be implemented')

    @property
    @abstractmethod
    def nested_items(self) -> Dict[str, Type[BaseItem]]:
        """
        Dictionary of the name and item in the collection
        """
        pass
