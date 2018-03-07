from typing import Any
from core.Interpreter import Interpreter
from core.FieldSchema import FieldSchema


class Field:
    def __init__(self, schema: FieldSchema) -> None:
        self.schema = schema
        self._initial_value = None
        self._value = None

    def initialize(self, value) -> None:
        self._initial_value = value
        self._value = value

    def changes(self) -> Any:
        if self._value != self._initial_value:
                self.schema.type.diff(
                    self._initial_value if self._initial_value is not None else self.schema.type.default,
                    self._value)

    def evaluate(self, interpreter: Interpreter) -> None:
        new_value = None
        if not self.schema.filter or interpreter.evaluate(self.schema.filter):
            new_value = interpreter.evaluate(self.schema.value)

        if not self.schema.type.type_of(new_value):
            # TODO Give more meaningful error name
            raise ValueError('Type mismatch')

        self._value = new_value

    @property
    def name(self):
        return self.schema.name

    @property
    def value(self):
        return self._value if self._value else self.schema.type.default
