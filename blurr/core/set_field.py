from typing import Any

from blurr.core.field import FieldSchema, ComplexTypeBase


class Set(ComplexTypeBase, set):
    """
    Extends native set with operations for evaluation support.
    """
    pass


class SetFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return Set

    @property
    def default(self) -> Any:
        return Set()
