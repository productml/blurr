from typing import Any

from blurr.core.field import FieldSchema


class Set(set):
    """
    Extends native set with operations for evaluation support.
    """

    def add(self, element):
        """ Adds an element to the set """
        super().add(element)
        return self

    def difference(self, *s):
        """ Return the difference of two or more sets as a new set. """
        return Set(super().difference(*s))

    def difference_update(self, *s):
        """ Remove all elements of another set from this set. """
        super().difference_update(*s)
        return self

    def intersection(self, *s):
        """
       Return the intersection of two sets as a new set.
        """
        return Set(super().intersection(*s))

    def intersection_update(self, *s):
        """ Update a set with the intersection of itself and another. """
        super().intersection_update(*s)
        return self

    def symmetric_difference(self, *s):
        """
       Return the symmetric difference of two sets as a new set.
        """
        return Set(super().symmetric_difference(*s))

    def symmetric_difference_update(self, *s):
        """ Update a set with the symmetric difference of itself and another. """
        super().symmetric_difference_update(*s)
        return self

    def union(self, *s):
        """ Return the union of sets as a new set. """
        return Set(super().union(*s))

    def update(self, *s):
        """ Update a set with the union of itself and others. """
        super().update(*s)
        return self

    def discard(self, element):
        """ Discard an element from a set if it is a member. If the element is not a member, do nothing. """
        super().discard(element)
        return self

    def remove(self, element):
        """ Remove an element from a set if it is a member. If the element is not a member, raise a KeyError. """
        super().remove(element)
        return self

    def clear(self):
        """ Clears all items form the set and returns itself """
        super().clear()
        return self

    def copy(self):
        """ Creates a copy of the set """
        return Set(super().copy())


class SetFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return Set

    @property
    def default(self) -> Any:
        return Set()
