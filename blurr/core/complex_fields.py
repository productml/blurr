from typing import Any

from blurr.core.field import FieldSchema, ComplexTypeBase


class Map(ComplexTypeBase, dict):
    """
    Extends native dictionary with operations for evaluation support.
    """

    def set(self, key: Any, value: Any) -> None:
        """ Sets the value of a key to a supplied value """
        self[key] = value

    def increment(self, key: Any, by: int = 1) -> None:
        """ Increments the value set against a key.  If the key is not present, 0 is assumed as the initial state """
        self[key] = self.get(key, 0) + by


class MapFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return Map

    @property
    def default(self) -> Any:
        return Map()


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
