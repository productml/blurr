from typing import Any

from blurr.core.field import FieldSchema, ComplexTypeBase


class List(ComplexTypeBase, list):
    """
    Extends native list with operations for evaluation support.
    """
    pass


class ListFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return List

    @property
    def default(self) -> Any:
        return List()
