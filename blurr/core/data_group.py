from abc import ABC
from typing import Any, Dict, Type

from blurr.core.base import BaseSchemaCollection, BaseItemCollection, BaseItem
from blurr.core.evaluation import EvaluationContext
from blurr.core.loader import TypeLoader
from blurr.core.store import Key


class DataGroupSchema(BaseSchemaCollection, ABC):
    """
    Group Schema must inherit from this base.  Data Group schema provides the
    abstraction for managing the 'Fields' in the group.
    """

    # Field Name Definitions
    ATTRIBUTE_STORE = 'Store'
    ATTRIBUTE_FIELDS = 'Fields'

    def __init__(self, spec: Dict[str, Any], stores: Dict[str, Any]) -> None:
        """
        Initializing the nested field schema that all data groups contain
        :param spec: Schema specifications for the field
        """
        super().__init__(spec, self.ATTRIBUTE_FIELDS)
        self.store = stores.get(spec.get(self.ATTRIBUTE_STORE, None), None)


class DataGroup(BaseItemCollection):
    """
    All Data Groups inherit from this base.  Provides an abstraction for 'value' of the encapsulated
    to be called as properties of the data group itself.
    """

    def __init__(self, schema: DataGroupSchema, identity: Any,
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
                item_schema, self.evaluation_context.fork)
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
