from typing import Any

from blurr.core.field import FieldSchema


class List(list):
    """
    Extends native list with operations for evaluation support.
    """

    def append(self, item):
        """ Appends an item to the list """
        super().append(item)
        return self

    def insert(self, index: int, item):
        """ Inserts an item at the specified index """
        super().insert(index, item)
        return self

    def extend(self, iterable):
        """ Extends itself with the supplied iterable """
        super().extend(iterable)
        return self

    def remove(self, item):
        """ Removes the specified item"""
        super().remove(item)
        return self

    def clear(self):
        """ Clears all items form the list """
        super().clear()
        return self

    def copy(self):
        """ Creates a copy of the list """
        return List(super().copy())


class ListFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return List

    @property
    def default(self) -> Any:
        return List()
