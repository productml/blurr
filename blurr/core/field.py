from core.interpreter import Interpreter

from typing import Any
from enum import Enum, auto
from datetime import datetime
from core.base import BaseSchema, BaseItem


class FieldSchema(BaseSchema):
    def __init__(self, schema: dict) -> None:
        super().__init__(schema)
        self.type = FieldTypes.load(schema['Type'])
        self.value = schema['Value']
        self.value_expr = compile(self.value, '<string>', 'eval')
        self.atomic = schema.get('Atomic', False)


FIELD_TYPE_VALIDATORS = {
    'string': lambda o: isinstance(o, str),
    'integer': lambda o: isinstance(o, int),
    'boolean': lambda o: isinstance(o, bool),
    'datetime': lambda o: isinstance(o, datetime),
    'float': lambda o: isinstance(o, float),
    'map': lambda o: isinstance(o, dict),
    'list': lambda o: isinstance(o, list),
    'set': lambda o: isinstance(o, set),
}

FIELD_TYPE_DEFAULTS = {
    'string': '',
    'integer': int(0),
    'boolean': False,
    'datetime': None,
    'float': float(0),
    'map': dict(),
    'list': list(),
    'set': set(),
}

FIELD_TYPE_DIFF = {
    'string': lambda old, new: new,
    'integer': lambda old, new: new - old,
    'boolean': lambda old, new: new,
    'datetime': lambda old, new: new,
    'float': lambda old, new: new - old,
    'map': lambda old, new: {k: new[k] for k in set(new) - set(old)},
    # TODO This does not handle the existence of duplicate values in list
    'list': lambda old, new: list(set(new) - set(old)),
    'set': lambda old, new: new - old,
}


class FieldTypes(Enum):
    string = auto()
    integer = auto()
    boolean = auto()
    datetime = auto()
    float = auto()
    map = auto()
    list = auto()
    set = auto()

    @staticmethod
    def load(name: str) -> 'FieldTypes':
        # TODO Throw meaningful error if the field does not exist
        return FieldTypes[name]

    def type_of(self, value: Any) -> bool:
        return FIELD_TYPE_VALIDATORS[self.name](value)

    @property
    def default(self) -> Any:
        return FIELD_TYPE_DEFAULTS[self.name]

    def diff(self, old: Any, new: Any) -> Any:
        return FIELD_TYPE_DIFF[self.name](old, new)


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
                self._schema.type.diff(
                    self._initial_value if self._initial_value is not None else self._schema.type.default,
                    self._value)

    def evaluate(self, interpreter: Interpreter) -> None:
        new_value = None
        if self.should_evaluate(interpreter):
            new_value = interpreter.evaluate(self._schema.value_expr)

        if not self._schema.type.type_of(new_value):
            # TODO Give more meaningful error name
            raise ValueError('Type mismatch')

        self._value = new_value

    @property
    def name(self):
        return self._schema.name

    @property
    def value(self):
        return self._value if self._value else self._schema.type.default
