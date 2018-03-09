from typing import Any, Dict
from blurr.core.context import Context
from blurr.core.field import Field, FieldSchema
from blurr.core.base import BaseItem, BaseSchema


class GroupSchema(BaseSchema):
    def __init__(self, schema: dict) -> None:
        super().__init__(schema)
        self.fields = {s['Name']: FieldSchema(s) for s in schema['Fields']}


class Group(BaseItem):
    def __init__(self, schema: GroupSchema, global_context: Context) -> None:
        super().__init__(schema, global_context)
        self._fields: Dict[str, Field] = {
            name: Field(field_schema, self._global_context,
                        self._local_context)
            for name, field_schema in schema.fields.items()
        }
        self._local_context.merge_context(self._fields)

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
    def name(self):
        return self._schema.name

    @property
    def fields(self):
        return self._fields

    @property
    def sub_items(self):
        return self._fields