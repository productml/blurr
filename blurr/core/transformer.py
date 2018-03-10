from typing import Any, Dict
from blurr.core.data_group import DataGroup
from blurr.core.base import BaseItemCollection, BaseItem, BaseSchemaCollection, Expression
from blurr.core.evaluation import Context
from blurr.core.loader import TypeLoader


class TransformerSchema(BaseSchemaCollection):
    ATTRIBUTE_VERSION = 'Version'
    ATTRIBUTE_DESCRIPTION = 'Description'
    ATTRIBUTE_IDENTITY = 'Identity'
    ATTRIBUTE_TIME = 'Time'
    ATTRIBUTE_DATA_GROUPS = 'DataGroups'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec, self.ATTRIBUTE_DATA_GROUPS)

    def validate(self, spec: Dict[str, Any]) -> None:
        # Ensure that the base validator is invoked
        super().validate(spec)

        # Validate schema specific attributes
        self.validate_required_attribute(spec, self.ATTRIBUTE_VERSION)
        self.validate_required_attribute(spec, self.ATTRIBUTE_DESCRIPTION)
        self.validate_required_attribute(spec, self.ATTRIBUTE_IDENTITY)
        self.validate_required_attribute(spec, self.ATTRIBUTE_TIME)

    def load(self, spec: Dict[str, Any]) -> None:
        # Ensure that the base loader is invoked
        super().load(spec)

        # Load the schema specific attributes
        self.version = spec[self.ATTRIBUTE_VERSION]
        self.description = spec[self.ATTRIBUTE_DESCRIPTION]
        self.identity = Expression(spec[self.ATTRIBUTE_IDENTITY])
        self.time = Expression(spec[self.ATTRIBUTE_TIME])

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
            for name, group_schema in schema.nested_schema.items()
        }
        self.global_context.merge_context(self._groups)

    def set_source_context(self, source_context: Context) -> None:
        self.global_context.merge_context(source_context)
        self.global_context.add_context('time',
                                        self.schema.time.evaluate(
                                            self.global_context))

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
