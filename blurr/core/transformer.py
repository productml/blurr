from typing import Any, Dict
from core.interpreter import Interpreter
from core.group import Group
from core.session_group import SessionGroup, SessionGroupSchema

GROUP_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': SessionGroup
}

GROUP_TYPE_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': SessionGroupSchema
}


class TransformerSchema:
    def __init__(self, schema: dict) -> None:
        self.schema = schema
        self.type = schema['Type']
        self.type = schema['Version']
        self.filter = schema['Filter']
        self.name = schema['Name']
        self.description = schema['Description']
        self.identity = schema['Identity']
        self.time = schema['Time']
        # TODO Write factory for loading the correct group schema form different types
        self.groups = {s['Name']: self.load_group_schema(s) for s in schema['Groups']}

    def load_group_schema(self, schema):
        # TODO Move the type name to type reference out to an external configuration

        return GROUP_TYPE_MAP[schema['Type']](schema)



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
