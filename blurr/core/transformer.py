from abc import ABC
from copy import copy
from typing import Dict

from blurr.core.aggregate import Aggregate
from blurr.core.base import BaseItemCollection, BaseSchemaCollection
from blurr.core.errors import MissingAttributeError
from blurr.core.loader import TypeLoader
from blurr.core.schema_context import SchemaContext
from blurr.core.schema_loader import SchemaLoader


class TransformerSchema(BaseSchemaCollection, ABC):
    """
    All Transformer Schema inherit from this base.  Adds support for handling
    the required attributes of a schema.
    """

    ATTRIBUTE_VERSION = 'Version'
    ATTRIBUTE_AGGREGATES = 'Aggregates'
    ATTRIBUTE_IMPORT = 'Import'
    ATTRIBUTE_STORES = 'Stores'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader, self.ATTRIBUTE_AGGREGATES)

        self.version = self._spec.get(self.ATTRIBUTE_VERSION, None)
        self.import_list = self._spec.get(self.ATTRIBUTE_IMPORT, [])
        self.schema_context = SchemaContext(self.import_list)

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_VERSION)
        self.validate_enum_attribute(self.ATTRIBUTE_VERSION, {'2018-03-01'})


class Transformer(BaseItemCollection, ABC):
    """
    All transformers inherit from this base.  Adds the current transformer
    to the context
    """

    def __init__(self, schema: TransformerSchema, identity: str) -> None:
        super().__init__(schema, copy(schema.schema_context.context))
        # Load the nested items into the item
        self._aggregates: Dict[str, Aggregate] = {
            name: TypeLoader.load_item(item_schema.type)(item_schema, identity,
                                                         self._evaluation_context)
            for name, item_schema in schema.nested_schema.items()
        }
        self._identity = identity
        self._evaluation_context.global_add('identity', self._identity)
        self._evaluation_context.global_context.merge(self._nested_items)

    @property
    def _nested_items(self) -> Dict[str, Aggregate]:
        """
        Dictionary of nested data groups
        """
        return self._aggregates

    def run_finalize(self) -> None:
        """
        Iteratively finalizes all data groups in its transformer
        """
        for item in self._nested_items.values():
            item.run_finalize()

    def __getattr__(self, item: str) -> Aggregate:
        """
        Makes the value of the nested items available as properties
        of the collection object.  This is used for retrieving data groups
        for dynamic execution.
        :param item: Aggregate requested
        """
        return self.__getitem__(item)

    def __getitem__(self, item) -> Aggregate:
        """
        Makes the nested items available though the square bracket notation.
        :raises KeyError: When a requested item is not found in nested items
        """
        if item not in self._nested_items:
            raise MissingAttributeError('{item} not defined in {name}'.format(
                item=item, name=self._name))

        return self._nested_items[item]
