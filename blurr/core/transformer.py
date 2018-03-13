from typing import Any, Dict
from abc import ABC

from blurr.core.base import BaseItemCollection, BaseSchemaCollection
from blurr.core.evaluation import Context, EvaluationContext


class TransformerSchema(BaseSchemaCollection, ABC):
    """
    All Transformer Schema inherit from this base.  Adds support for handling
    the required attributes of a schema.
    """

    ATTRIBUTE_VERSION = 'Version'
    ATTRIBUTE_DESCRIPTION = 'Description'
    ATTRIBUTE_DATA_GROUPS = 'DataGroups'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec, self.ATTRIBUTE_DATA_GROUPS)

    def validate(self, spec: Dict[str, Any]) -> None:
        # Ensure that the base validator is invoked
        super().validate(spec)

        # Validate schema specific attributes
        self.validate_required_attribute(spec, self.ATTRIBUTE_VERSION)
        self.validate_required_attribute(spec, self.ATTRIBUTE_DESCRIPTION)

    def load(self, spec: Dict[str, Any]) -> None:
        # Ensure that the base loader is invoked
        super().load(spec)

        # Load the schema specific attributes
        self.version = spec[self.ATTRIBUTE_VERSION]
        self.description = spec[self.ATTRIBUTE_DESCRIPTION]


class Transformer(BaseItemCollection, ABC):
    """
    All transformers inherit from this base.  Adds the current transformer
    to the context
    """

    def __init__(self, schema: TransformerSchema,  evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, evaluation_context)
        self.evaluation_context.global_add(self.name, self)
        self.evaluation_context.global_context.merge(self.nested_items)
