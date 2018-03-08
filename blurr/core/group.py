from typing import Any, Dict
from core.interpreter import Interpreter
from core.field import Field, FieldSchema


class GroupSchema:
    def __init__(self, schema: dict) -> None:
        self.schema = schema
        self.name = schema['Name']
        self.type = schema['Type']
        self.filter = schema['Filter']
        self.fields = {s['Name']: FieldSchema(s) for s in schema['Fields']}


class Group:
    def __init__(self, schema: GroupSchema) -> None:
        self._schema = schema
        self._fields: Dict[str, Field] = {name: Field(field_schema) for name, field_schema in schema.fields}

    def initialize(self, field_values: Dict[str, Any]) -> None:
        for name, value in field_values:
            self.fields[name].initialize(value)

    def changes(self) -> Any:
        return {name: field.changes() for name, field in self.fields}

    def evaluate(self, interpreter: Interpreter) -> None:
        if not self._schema.filter or interpreter.evaluate(self._schema.filter):
            for _, field in self.fields:
                field.evaluate(interpreter)

    def __getattr__(self, item):
        if item in self._fields:
            return self._fields[item].value

        self.__getattribute__(item)

    @property
    def name(self):
        return self._schema.name

    @property
    def fields(self):
        return self.fields
