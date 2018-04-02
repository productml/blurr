from typing import Dict, Type

from abc import ABC

from blurr.core.base import BaseItemCollection, BaseSchemaCollection, BaseItem
from blurr.core.data_group import DataGroup
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Store


class TransformerSchema(BaseSchemaCollection, ABC):
    """
    All Transformer Schema inherit from this base.  Adds support for handling
    the required attributes of a schema.
    """

    ATTRIBUTE_VERSION = 'Version'
    ATTRIBUTE_DESCRIPTION = 'Description'
    ATTRIBUTE_STORES = 'Stores'
    ATTRIBUTE_DATA_GROUPS = 'DataGroups'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader,
                         self.ATTRIBUTE_DATA_GROUPS)

        # Load the schema specific attributes
        self.version = self._spec[self.ATTRIBUTE_VERSION]
        self.description = self._spec[
            self.
            ATTRIBUTE_DESCRIPTION] if self.ATTRIBUTE_DESCRIPTION in self._spec else None

        # Load list of stores from the schema
        self.stores: Dict[str, Type[Store]] = {
            schema_spec[self.ATTRIBUTE_NAME]:
            self.schema_loader.get_nested_schema_object(
                self.fully_qualified_name, schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec.get(self.ATTRIBUTE_STORES, [])
        }

        # Load nested schema items
        self.nested_schema: Dict[str, Type[DataGroup]] = {
            schema_spec[self.ATTRIBUTE_NAME]:
            self.schema_loader.get_nested_schema_object(
                self.fully_qualified_name, schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec[self._nested_item_attribute]
        }


class Transformer(BaseItemCollection, ABC):
    """
    All transformers inherit from this base.  Adds the current transformer
    to the context
    """

    def __init__(self, schema: TransformerSchema, identity: str,
                 context: Context) -> None:
        super().__init__(schema, EvaluationContext(global_context=context))
        # Load the nested items into the item
        self._data_groups: Dict[str, Type[BaseItem]] = {
            name: TypeLoader.load_item(item_schema.type)(
                item_schema, identity, self.evaluation_context)
            for name, item_schema in schema.nested_schema.items()
        }
        self.identity = identity
        self.evaluation_context.global_add('identity', self.identity)
        self.evaluation_context.global_context.merge(self.nested_items)

    @property
    def nested_items(self) -> Dict[str, Type[BaseItem]]:
        """
        Dictionary of nested data groups
        """
        return self._data_groups

    def finalize(self) -> None:
        """
        Iteratively finalizes all data groups in its transformer
        """
        for item in self.nested_items.values():
            item.finalize()
