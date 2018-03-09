from abc import ABC
from typing import Any, Dict

from blurr.core.base import BaseSchema, BaseItemCollection
from blurr.core.evaluation import Context
from blurr.core.field import Field, FieldSchema


class DataGroupSchema(BaseSchema, ABC):
    """
    Base for individual Group schema.  The Base implements 'Fields' which represents the list of
    'Field' schema objects.
    """

    # Field Name Definitions
    FIELD_FIELDS = 'Fields'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)

    def validate(self, spec: Dict[str, Any]) -> None:
        self.validate_required_attribute(spec, self.FIELD_FIELDS)

        if not isinstance(spec[self.FIELD_FIELDS], list):
            self.raise_validation_error(spec, self.FIELD_FIELDS, 'Must be a list of fields.')

        if len(spec[self.FIELD_FIELDS]) == 0:
            self.raise_validation_error(spec, self.FIELD_FIELDS, 'Must contain at least 1 field.')

    def load(self, spec: Dict[str, Any]) -> None:
        self.fields: Dict[str, FieldSchema] = {}
        for field_spec in spec[self.FIELD_FIELDS]:
            field_schema = FieldSchema(field_spec)
            self.fields[field_schema.name] = field_schema


class DataGroup(BaseItemCollection):
    def __init__(self, schema: DataGroupSchema, global_context: Context) -> None:
        super().__init__(schema, global_context)
        self._fields: Dict[str, Field] = {
            name: Field(field_schema, self.global_context,
                        self.local_context)
            for name, field_schema in schema.fields.items()
        }
        self.local_context.merge_context(self._fields)

    def initialize(self, field_values: Dict[str, Any]) -> None:
        for name, value in field_values:
            self._fields[name].initialize(value)

    def changes(self) -> Any:
        return {name: field.changes() for name, field in self.fields}

    def __getattr__(self, item):
        if item in self._fields:
            return self._fields[item].value

        self.__getattribute__(item)

    @property
    def fields(self):
        return self._fields

    @property
    def items(self):
        return self._fields
