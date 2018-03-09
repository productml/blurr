from typing import Any, Dict
from blurr.core.data_group import DataGroup
from blurr.core.base import BaseItemCollection, BaseItem, BaseSchema, Expression
from blurr.core.evaluation import Context
from blurr.core.loader import TypeLoader


class TransformerSchema(BaseSchema):
    FIELD_VERSION = 'Version'
    FIELD_DESCRIPTION = 'Description'
    FIELD_IDENTITY = 'Identity'
    FIELD_TIME = 'Time'
    FIELD_GROUPS = 'DataGroups'

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


class Transformer(BaseItemCollection):
    def __init__(self, schema: TransformerSchema, identity,
                 exec_context: Context) -> None:
        super().__init__(schema)
        self.global_context.add_context(self.name, self)
        self.global_context.merge_context(exec_context)
        self._identity = identity
        self.global_context.add_context('identity', self._identity)
        self._groups: Dict[str, DataGroup] = {
            name: TypeLoader.load_item(group_schema.type)(group_schema,
                                                          self.global_context)
            for name, group_schema in schema.groups.items()
        }
        self.global_context.merge_context(self._groups)

    def set_source_context(self, source_context: Context) -> None:
        self.global_context.merge_context(source_context)
        self.global_context.add_context('time', self.schema.time.evaluate(self.global_context))

    @property
    def items(self) -> Dict[str, BaseItemCollection]:
        return self._groups

    def __getattr__(self, item):
        if item in self._groups:
            return self._groups[item].value

        self.__getattribute__(item)

    @property
    def groups(self):
        return self._groups

