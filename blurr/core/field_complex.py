from typing import Any

from blurr.core.field import FieldSchema, ComplexTypeBase


class Map(ComplexTypeBase, dict):
    """
    Extends native dictionary with operations for evaluation support.
    """

    def set(self, key: Any, value: Any) -> None:
        """ Sets the value of a key to a supplied value """
        if key is not None:
            self[key] = value

    def increment(self, key: Any, by: int = 1) -> None:
        """ Increments the value set against a key.  If the key is not present, 0 is assumed as the initial state """
        if key is not None:
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

    def append(self, obj: Any) -> None:
        """ Appends an object to the list as long as it is not None """
        if obj is not None:
            super().append(obj)

    def insert(self, index: int, obj: Any) -> None:
        """ Inserts an item to the list as long as it is not None """
        if obj is not None:
            super().insert(index, obj)


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

    def add(self, element: Any) -> None:
        """ Adds an element to the set as long as it is not None """
        if element is not None:
            super().add(element)


class SetFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return Set

    @property
    def default(self) -> Any:
        return Set()

    @staticmethod
    def encoder(value: Any) -> Set:
        return list(value)

    @staticmethod
    def decoder(value: Any) -> Set:
        return Set(value)
