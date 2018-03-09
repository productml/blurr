from typing import Any, Dict
from blurr.core.data_group import DataGroup
from blurr.core.base import BaseItem, BaseSchema, Expression
from blurr.core.evaluation import Context
from blurr.core.loader import TypeLoader


class TransformerSchema(BaseSchema):
    FIELD_VERSION = 'Version'
    FIELD_DESCRIPTION = 'Description'
    FIELD_IDENTITY = 'Identity'
    FIELD_TIME = 'Time'
    FIELD_GROUPS = 'Groups'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)

    def validate(self, spec: Dict[str, Any]) -> None:
        self.validate_required_attribute(spec, self.FIELD_VERSION)
        self.validate_required_attribute(spec, self.FIELD_DESCRIPTION)
        self.validate_required_attribute(spec, self.FIELD_IDENTITY)
        self.validate_required_attribute(spec, self.FIELD_TIME)

    def load(self, spec: Dict[str, Any]) -> None:
        self.version = spec[self.FIELD_VERSION]
        self.description = spec[self.FIELD_DESCRIPTION]
        self.identity = Expression(spec[self.FIELD_IDENTITY])
        self.time = Expression(spec[self.FIELD_TIME])
        self.groups = {group_spec[self.FIELD_NAME]: TypeLoader.load_schema(group_spec[self.FIELD_TYPE])(group_spec)
                       for group_spec in spec[self.FIELD_GROUPS]}

    def get_identity(self, source_context: Context):
        return self.identity.evaluate(source_context)


class Transformer(BaseItem):
    def __init__(self, schema: TransformerSchema, identity,
                 exec_context: Context) -> None:
        super().__init__(schema)
        self.global_context.add_context(self.name, self)
        self.global_context.merge_context(exec_context)
        self._identity = identity
        self.global_context.add_context('identity', self._identity)
        self._groups: Dict[str, DataGroup] = {
            name: self.load_group(group_schema)
            for name, group_schema in schema.groups.items()
        }
        self.global_context.merge_context(self._groups)

    def load_group(self, schema):
        # TODO Move the type name to type reference out to an external configuration
        return GROUP_MAP[schema.type](schema, self.global_context)

    def set_source_context(self, source_context: Context) -> None:
        self.global_context.merge_context(source_context)
        self.global_context.add_context('time',
                                        self.evaluate_expr(
                                             self.schema.time_expr))

    @property
    def items(self) -> BaseItem:
        return self._groups

    def __getattr__(self, item):
        if item in self._groups:
            return self._groups[item].value

        self.__getattribute__(item)

    @property
    def name(self):
        return self.schema.name

    @property
    def groups(self):
        return self._groups

