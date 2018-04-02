from typing import Dict, Type

from abc import ABC

from blurr.core.base import BaseSchemaCollection, BaseItemCollection, BaseItem
from blurr.core.evaluation import EvaluationContext
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Key


class DataGroupSchema(BaseSchemaCollection, ABC):
    """
    Group Schema must inherit from this base.  Data Group schema provides the
    abstraction for managing the 'Fields' in the group.
    """

    # Field Name Definitions
    ATTRIBUTE_STORE = 'Store'
    ATTRIBUTE_FIELDS = 'Fields'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        """
        Initializing the nested field schema that all data groups contain
        :param spec: Schema specifications for the field
        """
        super().__init__(fully_qualified_name, schema_loader,
                         self.ATTRIBUTE_FIELDS)
        self.store = None
        if self.ATTRIBUTE_STORE in self._spec:
            self.store = self._load_store(self._spec[self.ATTRIBUTE_STORE])

    def _load_store(self, store_name: str) -> 'Store':
        """
        Load a store into the datagroup
        :param store_name: The name of the store
        """
        store_fq_name = self.schema_loader.get_fully_qualified_name(
            self.schema_loader.get_transformer_name(self.fully_qualified_name),
            store_name)
        return self.schema_loader.get_schema_object(store_fq_name)


class DataGroup(BaseItemCollection, ABC):
    """
    All Data Groups inherit from this base.  Provides an abstraction for 'value' of the encapsulated
    to be called as properties of the data group itself.
    """

    def __init__(self, schema: DataGroupSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        """
        Initializes the data group with the inherited context and adds
        its own nested items into the local context for execution
        :param schema: Schema for initializing the data group
        :param evaluation_context: Context dictionary for evaluation
        """
        super().__init__(schema, evaluation_context)
        self.identity = identity

        self._fields: Dict[str, Type[BaseItem]] = {
            name: TypeLoader.load_item(item_schema.type)(
                item_schema, self.evaluation_context)
            for name, item_schema in self.schema.nested_schema.items()
        }

    @property
    def nested_items(self) -> Dict[str, Type[BaseItem]]:
        """
        Returns the dictionary of fields the DataGroup contains
        """
        return self._fields

    def finalize(self) -> None:
        """
        Saves the current state of the DataGroup in the store as the final rites
        """
        self.persist()

    def persist(self, timestamp=None) -> None:
        """
        Persists the current data group
        :param timestamp: Optional timestamp to include in the Key construction
        """
        if self.schema.store:
            self.schema.store.save(
                Key(self.identity, self.name, timestamp), self.snapshot)
