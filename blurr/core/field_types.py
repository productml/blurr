from typing import Any
from datetime import datetime

from blurr.core.base import BaseType


class IntegerType(BaseType):

    @property
    def type_object(self) -> Any:
        return int

    @property
    def default(self) -> Any:
        return int(0)

    def calculate_difference(self, old: Any, new: Any) -> Any:
        return new - old


class FloatType(BaseType):

    @property
    def type_object(self) -> Any:
        return float

    @property
    def default(self) -> Any:
        return float(0)

    def calculate_difference(self, old: Any, new: Any) -> Any:
        return new - old


class StringType(BaseType):

    @property
    def type_object(self) -> Any:
        return str

    @property
    def default(self) -> Any:
        return str()

    def calculate_difference(self, old: Any, new: Any) -> Any:
        return new


class BooleanType(BaseType):

    @property
    def type_object(self) -> Any:
        return bool

    @property
    def default(self) -> Any:
        return False

    def calculate_difference(self, old: Any, new: Any) -> Any:
        return new


class DateTimeType(BaseType):

    @property
    def type_object(self) -> Any:
        return datetime

    @property
    def default(self) -> Any:
        return None

    def calculate_difference(self, old: Any, new: Any) -> Any:
        return new


class DateTimeType(BaseType):

    @property
    def type_object(self) -> Any:
        return datetime

    @property
    def default(self) -> Any:
        return None

    def calculate_difference(self, old: Any, new: Any) -> Any:
        return new


class MapType(BaseType):

    @property
    def type_object(self) -> Any:
        return dict

    @property
    def default(self) -> Any:
        return dict()

    def calculate_difference(self, old: Any, new: Any) -> Any:
        return {k: new[k] for k in set(new) - set(old)}


class ListType(BaseType):

    @property
    def type_object(self) -> Any:
        return list

    @property
    def default(self) -> Any:
        return list()

    def calculate_difference(self, old: Any, new: Any) -> Any:
        # TODO Duplicates and order will not be persisted. This needs rework.
        return list(set(new) - set(old))


class SetType(BaseType):

    @property
    def type_object(self) -> Any:
        return set

    @property
    def default(self) -> Any:
        return set()

    def calculate_difference(self, old: Any, new: Any) -> Any:
        # TODO Duplicates and order will not be persisted. This needs rework.
        return new - old
