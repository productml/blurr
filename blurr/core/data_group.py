from abc import ABC
from typing import Any, Dict

from blurr.core.base import BaseSchemaCollection, BaseItemCollection
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.schema_loader import SchemaLoader


class DataGroupSchema(BaseSchemaCollection, ABC):
    """
    Group Schema must inherit from this base.  Data Group schema provides the
    abstraction for managing the 'Fields' in the group.
    """

    # Field Name Definitions
    ATTRIBUTE_FIELDS = 'Fields'

    def __init__(self, name: str, schema_loader: SchemaLoader) -> None:
        """
        Initializing the nested field schema that all data groups contain
        :param spec: Schema specifications for the field
        """
        super().__init__(name, schema_loader, self.ATTRIBUTE_FIELDS)


class DataGroup(BaseItemCollection):
    """
    All Data Groups inherit from this base.  Provides an abstraction for 'value' of the encapsulated
    to be called as properties of the data group itself.
    """

    def __init__(self, schema: DataGroupSchema,
                 evaluation_context: EvaluationContext) -> None:
        """
        Initializes the data group with the inherited context and adds
        its own nested items into the local context for execution
        :param schema: Schema for initializing the data group
        :param evaluation_context: Context dictionary for evaluation
        """
        super().__init__(schema, evaluation_context)