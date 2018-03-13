from abc import ABC
from typing import Any, Dict

from blurr.core.base import BaseSchemaCollection, BaseItemCollection
from blurr.core.evaluation import Context


class DataGroupSchema(BaseSchemaCollection, ABC):
    """
    Group Schema must inherit from this base.  Data Group schema provides the
    abstraction for managing the 'Fields' in the group.
    """

    # Field Name Definitions
    ATTRIBUTE_FIELDS = 'Fields'

    def __init__(self, spec: Dict[str, Any]) -> None:
        """
        Initializing the nested field schema that all data groups contain
        :param spec: Schema specifications for the field
        """
        super().__init__(spec, self.ATTRIBUTE_FIELDS)


class DataGroup(BaseItemCollection):
    """
    All Data Groups inherit from this base.  Provides an abstraction for 'value' of the encapsulated
    to be called as properties of the data group itself.
    """

    def __init__(self,
                 schema: DataGroupSchema,
                 global_context: Context = Context(),
                 local_context: Context = Context()) -> None:
        """
        Initializes the data group with the inherited context and adds
        its own nested items into the local context for execution
        :param schema: Schema for initializing the data group
        :param global_context: Inherited Global context
        :param local_context: Inherited Local context to which the current
        data group's nested items are added.
        """
        super().__init__(schema, global_context, local_context)

        self.local_context.merge(self.nested_items)
