from typing import Any

from blurr.core.field import FieldSchema


class List(list):
    """
    Implements a list object that extends native list with operations for evaluation support.
    """

    def append(self, object):
        """
        Appends an object to the list
        :param object: Object to append
        :return: The list itself
        """
        super().append(object)
        return self

    def insert(self, index: int, object):
        """
        Inserts an object at the specified index and returns itself
        :param index: Index at which to insert
        :param object: Object to insert
        :return: The list itself
        """
        super().insert(index, object)
        return self

    def extend(self, iterable):
        """
        Extends itself with the supplied iterable and returns itself
        :param iterable: Iterable to extend itself with
        :return: The list itself
        """
        super().extend(iterable)
        return self

    def remove(self, object):
        """
        Removes the specified object and returns itself
        :param object: Object in the list to remove
        :return: The list itself
        """
        super().remove(object)
        return self

    def clear(self):
        """
        Clears all items form the list and returns itself
        :return: The list itself
        """
        super().clear()
        return self

    def copy(self):
        """
        Creates a copy of the list
        :return: Copy of the list as a List object
        """
        return List(super().copy())


class ListFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return List

    @property
    def default(self) -> Any:
        return List()
