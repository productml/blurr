from abc import ABC
from typing import Any, Dict

from blurr.core.base import BaseSchemaCollection, BaseItemCollection
from blurr.core.evaluation import Context
from blurr.core.field import Field, FieldSchema
from blurr.core.loader import TypeLoader


class DataGroupSchema(BaseSchemaCollection, ABC):
    """
    Base for Group schema
    """

    # Field Name Definitions
    ATTRIBUTE_FIELDS = 'Fields'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec, self.ATTRIBUTE_FIELDS)


class DataGroup(BaseItemCollection):
    def __init__(self, schema: DataGroupSchema,
                 global_context: Context) -> None:
        super().__init__(schema, global_context)
        self._fields: Dict[str, Field] = {
            name: TypeLoader.load_item(field_schema.type)(
                field_schema, self.global_context, self.local_context)
            for name, field_schema in schema.nested_schema.items()
        }
        self.local_context.merge_context(self._fields)

    def initialize(self, field_values: Dict[str, Any]) -> None:
        for name, value in field_values:
            self._fields[name].initialize(value)

    def changes(self) -> Any:
        return {name: field.changes() for name, field in self.fields}

    def __getattr__(self, item):
        if item in self._fields:
            return self._fields[item].value

        self.__getattribute__(item)

    @property
    def fields(self):
        return self._fields

    @property
    def items(self):
        return self._fields
