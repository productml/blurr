from typing import Any, Dict
from core.Interpreter import Interpreter
from core.TransformerSchema import TransformerSchema
from core.Group import Group
from core.SessionGroup import SessionGroup

GROUP_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': SessionGroup
}


class Transformer:
    def __init__(self, schema: TransformerSchema, identity) -> None:
        self._schema = schema
        self._identity = identity
        self._groups: Dict[str, Group] = {name: self.load_group(group_schema) for name, group_schema in schema.groups}

    def load_group(self, schema):
        # TODO Move the type name to type reference out to an external configuration
        return GROUP_MAP[schema.type](schema)

    def evaluate(self, interpreter: Interpreter) -> None:
        if not self._schema.filter or interpreter.evaluate(self._schema.filter):
            for _, group in self.groups:
                group.evaluate(interpreter)

    def __getattr__(self, item):
        if item in self._groups:
            return self._groups[item].value

        self.__getattribute__(item)

    @property
    def name(self):
        return self._schema.name

    @property
    def groups(self):
        return self._groups
