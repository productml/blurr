from typing import Any

from blurr.core.field import FieldSchema


class Map(dict):
    """
    Extends native dictionary with operations for evaluation support.
    """

    def set(self, key, value):
        """ Sets the value of a key to a supplied value """
        self[key] = value
        return self

    def increment(self, key, by=1):
        """ Increments the value set against a key.  If the key is not present, 0 is assumed as the initial state """
        self[key] = self.get(key, 0) + by
        return self

    def update(self, m, **kwargs):
        """ Updates the map with another map """
        super().update(m, **kwargs)
        return self

    def clear(self):
        """ Clears all keys and values form the map and returns itself """
        super().clear()
        return self

    def copy(self):
        """ Creates a copy of the map """
        return Map(super().copy())


class MapFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return Map

    @property
    def default(self) -> Any:
        return Map()
