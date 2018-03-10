from abc import ABC
from typing import Any, Dict

from blurr.core.base import BaseSchemaCollection, BaseItemCollection
from blurr.core.evaluation import Context


class DataGroupSchema(BaseSchemaCollection, ABC):
    """
    Base for Group schema
    """

    # Field Name Definitions
    ATTRIBUTEitems = 'Fields'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec, self.ATTRIBUTEitems)


class DataGroup(BaseItemCollection):
    def __init__(self, schema: DataGroupSchema,
                 global_context: Context = Context(),
                 local_context: Context = Context()) -> None:

        super().__init__(schema, global_context, local_context)

        self.local_context.merge_context(self.items)

    def initialize(self, field_values: Dict[str, Any]) -> None:
        for name, value in field_values:
            self.items[name].initialize(value)

    def __getattr__(self, item):
        if item in self.items:
            return self.items[item].value

        self.__getattribute__(item)
