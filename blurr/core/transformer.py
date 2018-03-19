from typing import Any, Dict
from abc import ABC

from blurr.core.base import BaseItemCollection, BaseSchemaCollection
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Store, Key


class TransformerSchema(BaseSchemaCollection, ABC):
    """
    All Transformer Schema inherit from this base.  Adds support for handling
    the required attributes of a schema.
    """

    ATTRIBUTE_VERSION = 'Version'
    ATTRIBUTE_DESCRIPTION = 'Description'
    ATTRIBUTE_DATA_GROUPS = 'DataGroups'

    def __init__(self, name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(name, schema_loader, self.ATTRIBUTE_DATA_GROUPS)

    def validate(self, spec: Dict[str, Any]) -> None:
        # Ensure that the base validator is invoked
        super().validate(spec)

        # Validate schema specific attributes
        self.validate_required_attribute(spec, self.ATTRIBUTE_VERSION)
        self.validate_required_attribute(spec, self.ATTRIBUTE_DESCRIPTION)

    def load(self) -> None:
        # Ensure that the base loader is invoked
        super().load()

        # Load the schema specific attributes
        self.version = self._spec[self.ATTRIBUTE_VERSION]
        self.description = self._spec[self.ATTRIBUTE_DESCRIPTION]


class Transformer(BaseItemCollection, ABC):
    """
    All transformers inherit from this base.  Adds the current transformer
    to the context
    """

    def __init__(self, store: Store, schema: TransformerSchema, identity: str,
                 context: Context) -> None:
        super().__init__(schema, EvaluationContext(global_context=context))
        self.store = store
        self.identity = identity
        self.evaluation_context.global_add('identity', self.identity)
        self.evaluation_context.global_context.merge(self.nested_items)

    def finalize(self):
        for item in self.nested_items.values():
            self.store.save(Key(self.identity, item.name), item.snapshot)
