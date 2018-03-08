from typing import Any
from core.Interpreter import Interpreter
from core.FieldSchema import FieldSchema
from core.BaseItem import  BaseItem


class Field(BaseItem):
    def __init__(self, schema: FieldSchema) -> None:
        super().__init__(schema)
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
        if self.should_evaluate(interpreter):
            new_value = interpreter.evaluate(self.schema.value_expr)

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
