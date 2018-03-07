from typing import Any
from enum import Enum, auto
from datetime import datetime


class FieldSchema:
    def __init__(self, schema: dict) -> None:
        self.name = schema['Name']
        self.type = FieldTypes.load(schema['Type'])
        self.filter = schema['Filter']
        self.value = schema['Value']
        self.atomic = schema['Atomic']


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
    def default(self):
        return FIELD_TYPE_DEFAULTS[self.name]

    def diff(self, old, new):
        return FIELD_TYPE_DIFF[self.name](old, new)
