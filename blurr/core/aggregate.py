from abc import ABC
from typing import Dict, Type, Any

from blurr.core.base import BaseSchemaCollection, BaseItemCollection, BaseItem
from blurr.core.errors import MissingAttributeError
from blurr.core.evaluation import EvaluationContext
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import StoreSchema, Store
from blurr.core.store_key import Key
from blurr.core.type import Type as DTCType
from blurr.core.validator import ATTRIBUTE_INTERNAL


class AggregateSchema(BaseSchemaCollection, ABC):
    """
    Group Schema must inherit from this base.  Data Group schema provides the
    abstraction for managing the 'Fields' in the group.
    """

    # Field Name Definitions
    ATTRIBUTE_STORE = 'Store'
    ATTRIBUTE_FIELDS = 'Fields'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        """
        Initializing the nested field schema that all data groups contain
        :param spec: Schema specifications for the field
        """
        super().__init__(fully_qualified_name, schema_loader, self.ATTRIBUTE_FIELDS)
        store_name = self._spec.get(self.ATTRIBUTE_STORE, None)
        self.store_schema: StoreSchema = None
        if store_name:
            self.store_schema = self.schema_loader.get_nested_schema_object(
                self.schema_loader.get_transformer_name(self.fully_qualified_name), store_name)

    def extend_schema_spec(self) -> None:
        """ Injects the identity field """
        super().extend_schema_spec()

        identity_field = {
            'Name': '_identity',
            'Type': DTCType.STRING,
            'Value': 'identity',
            ATTRIBUTE_INTERNAL: True
        }

        if self.ATTRIBUTE_FIELDS in self._spec:
            self._spec[self.ATTRIBUTE_FIELDS].insert(0, identity_field)
            self.schema_loader.add_schema_spec(identity_field, self.fully_qualified_name)


class Aggregate(BaseItemCollection, ABC):
    """
    All Data Groups inherit from this base.  Provides an abstraction for 'value' of the encapsulated
    to be called as properties of the data group itself.
    """

    def __init__(self, schema: AggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        """
        Initializes the data group with the inherited context and adds
        its own nested items into the local context for execution
        :param schema: Schema for initializing the data group
        :param evaluation_context: Context dictionary for evaluation
        """
        super().__init__(schema, evaluation_context)
        self._identity = identity

        self._fields: Dict[str, Type[BaseItem]] = {
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.nested_schema.items()
        }
        self._store: Store = None
        if self._schema.store_schema:
            self._store = self._schema.schema_loader.get_store(
                self._schema.store_schema.fully_qualified_name)

    @property
    def _nested_items(self) -> Dict[str, Type[BaseItem]]:
        """
        Returns the dictionary of fields the Aggregate contains
        """
        return self._fields

    def run_finalize(self) -> None:
        """
        Saves the current state of the Aggregate in the store as the final rites
        """
        self._persist()

    def _persist(self, timestamp=None) -> None:
        """
        Persists the current data group
        :param timestamp: Optional timestamp to include in the Key construction
        """
        if self._store:
            self._store.save(Key(self._identity, self._name, timestamp), self._snapshot)

    def __getattr__(self, item: str) -> Any:
        """
        Makes the value of the nested items available as properties
        of the collection object.  This is used for retrieving field values
        for dynamic execution.
        :param item: Field requested
        """
        return self.__getitem__(item)

    def __getitem__(self, item: str) -> Any:
        """
        Makes the nested items available though the square bracket notation.
        :raises KeyError: When a requested item is not found in nested items
        """
        if item not in self._nested_items:
            raise MissingAttributeError('{item} not defined in {name}'.format(
                item=item, name=self._name))

        return self._nested_items[item].value
