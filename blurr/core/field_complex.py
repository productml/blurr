from base64 import b64encode, b64decode
from io import BytesIO
from typing import Any

from pybloom_live import ScalableBloomFilter

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


class BloomFilter(ComplexTypeBase, ScalableBloomFilter):
    def __init__(self):
        super().__init__(mode=ScalableBloomFilter.LARGE_SET_GROWTH)

    def dump(self) -> str:
        file = BytesIO()
        self.tofile(file)
        return b64encode(file.getvalue()).decode("utf-8")

    @classmethod
    def load(cls, dump: str) -> 'BloomFilter':
        file = BytesIO(b64decode(dump))
        bf = cls.fromfile(file)
        bf.__class__ = cls
        return bf


class BloomFilterFieldSchema(FieldSchema):
    @property
    def type_object(self) -> Any:
        return BloomFilter

    @property
    def default(self) -> Any:
        return BloomFilter()

    @staticmethod
    def encoder(value: BloomFilter) -> str:
        return value.dump()

    def decoder(self, value: Any) -> Any:
        return self.type_object.load(value)
