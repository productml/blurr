from abc import ABC
from typing import Any, Dict

from blurr.core.base import BaseSchemaCollection, BaseItemCollection
from blurr.core.evaluation import Context


class DataGroupSchema(BaseSchemaCollection, ABC):
    """
    Base for DataGroup schema
    """

    # Field Name Definitions
    ATTRIBUTE_FIELD = 'Fields'

    def __init__(self, spec: Dict[str, Any]) -> None:
        """
        Initializing the nested field schema that all data groups contain
        :param spec:
        """
        super().__init__(spec, self.ATTRIBUTE_FIELD)


class DataGroup(BaseItemCollection):
    def __init__(self, schema: DataGroupSchema,
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

        self.local_context.merge_context(self.items)

    def __getattr__(self, item: str) -> Any:
        """
        Makes the value of the nested items available as properties
        of the data group object.  This is used for retrieving field values
        for dynamic execution.
        :param item: Field requested
        """
        if item in self.items:
            return self.items[item].snapshot

        self.__getattribute__(item)
