from typing import Any, Dict
from blurr.core.group import Group
from blurr.core.session_group import SessionGroup, SessionGroupSchema
from blurr.core.base import BaseItem, BaseSchema
from blurr.core.context import Context

GROUP_MAP = {'ProductML:DTC:DataGroup:SessionAggregate': SessionGroup}

GROUP_TYPE_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': SessionGroupSchema
}


class TransformerSchema(BaseSchema):
    def __init__(self, schema: dict) -> None:
        super().__init__(schema)
        self.type = schema['Version']
        self.description = schema['Description']
        self.identity = schema['Identity']
        self.identity_expr = compile(self.identity, '<string>', 'eval')
        self.time = schema['Time']
        self.time_expr = compile(self.time, '<string>', 'eval')
        # TODO Write factory for loading the correct group schema form different types
        self.groups = {
            s['Name']: self.load_group_schema(s)
            for s in schema['DataGroups']
        }

    def get_identity(self, source_context: Context):
        return eval(self.identity_expr, source_context)

    def load_group_schema(self, schema):
        # TODO Move the type name to type reference out to an external configuration

        return GROUP_TYPE_MAP[schema['Type']](schema)


class Transformer(BaseItem):
    def __init__(self, schema: TransformerSchema, identity,
                 exec_context: Context) -> None:
        super().__init__(schema)
        self._global_context.add_context(self.name, self)
        self._global_context.merge_context(exec_context)
        self._identity = identity
        self._global_context.add_context('identity', self._identity)
        self._groups: Dict[str, Group] = {
            name: self.load_group(group_schema)
            for name, group_schema in schema.groups.items()
        }
        self._global_context.merge_context(self._groups)

    def load_group(self, schema):
        # TODO Move the type name to type reference out to an external configuration
        return GROUP_MAP[schema.type](schema, self._global_context)

    def set_source_context(self, source_context: Context) -> None:
        self._global_context.merge_context(source_context)
        self._global_context.add_context('time',
                                         self.evaluate_expr(
                                             self._schema.time_expr))

    @property
    def sub_items(self) -> BaseItem:
        return self._groups

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

