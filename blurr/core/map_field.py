from typing import Any

from blurr.core.field import FieldSchema


class Map(dict):
    """
    Implements a map object that extends native dictionary with operations for evaluation support.
    """

    def set(self, key, value):
        """
        Sets the value of a key to a supplied value and returns itself
        :param key: Key to be set
        :param value: Value for the key
        :return: The object itself
        """
        self[key] = value
        return self

    def increment(self, key, by=1):
        """
        Increments the value set against a key.  If the key is not present, 0 is assumed as the initial state
        :param key: Key to change the value for
        :param by: Unit to increment the value by
        :return: The object itself
        """
        self[key] = self.get(key, 0) + by
        return self

    def update(self, m, **kwargs):
        """
        Updates the map with another map and returns the updated current map
        :param m: New map to update current map with
        :param kwargs: Additional arguments
        :return: The object itself
        """
        super().update(m, **kwargs)
        return self

    def clear(self):
        """
        Clears all keys and values form the map and returns itself
        :return: The object itself
        """
        super().clear()
        return self


class MapFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return Map

    @property
    def default(self) -> Any:
        return Map()
