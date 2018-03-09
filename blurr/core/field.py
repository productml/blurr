from datetime import datetime
from enum import Enum, auto
from typing import Dict, Any

from blurr.core.base import BaseSchema, BaseItem, Expression, BaseType
from blurr.core.context import Context
from blurr.core.loader import TypeLoader


class FieldSchema(BaseSchema):
    """
    An individual field schema.
        1. Field Schema must be defined inside a Group
        2. Contain the attributes:
            a. Name (inherited from base)
            b. Type (inherited from base)
            c. Value (required)
            d. Filter (inherited from base)
    """

    # Field Name Definitions
    FIELD_VALUE = 'Value'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)

    def validate(self, spec: Dict[str, Any]) -> None:
        self.validate_required_attribute(spec, self.FIELD_VALUE)

    def load(self, spec: Dict[str, Any]) -> None:
        self.type: BaseType = TypeLoader.load_type(spec[self.FIELD_TYPE])
        self.value: Expression = Expression(spec[self.FIELD_VALUE])


class Field(BaseItem):
    def __init__(self, schema: FieldSchema, global_context: Context,
                 local_context: Context) -> None:
        super().__init__(schema, global_context, local_context)
        self._initial_value = None
        self._value = None

    def initialize(self, value) -> None:
        self._initial_value = value
        self._value = value

    def changes(self) -> Any:
        if self._value != self._initial_value:
            self.schema.type.diff(self._initial_value
                                   if self._initial_value is not None else
                                   self.schema.type.default, self._value)

    def evaluate(self) -> None:
        new_value = None
        if self.should_evaluate():
            new_value = self.evaluate_expr(self.schema.value_expr)

        if not self.schema.type.is_type_of(new_value):
            # TODO Give more meaningful error name
            raise ValueError('Type mismatch')

        self._value = new_value

    @property
    def name(self):
        return self.schema.name

    @property
    def value(self):
        return self._value if self._value else self.schema.type.default
