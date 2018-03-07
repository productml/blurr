from typing import Any, Dict
from core.Interpreter import Interpreter
from core.DataGroupSchema import DataGroupSchema
from core.Field import Field


class DataGroup:
    def __init__(self, schema: DataGroupSchema) -> None:
        self.schema = schema
        self.fields: Dict[str, Field] = {name: Field(field_schema) for name, field_schema in schema.fields}

    def initialize(self, field_values: Dict[str, Any]) -> None:
        for name, value in field_values:
            self.fields[name].initialize(value)

    def changes(self) -> Any:
        return {name: field.changes() for name, field in self.fields}

    def evaluate(self, interpreter: Interpreter) -> None:
        if not self.schema.filter or interpreter.evaluate(self.schema.filter):
            for _, field in self.fields:
                field.evaluate(interpreter)

    @property
    def name(self):
        return self.schema.name

    @property
    def fields(self):
        return self.fields
