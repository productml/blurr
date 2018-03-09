from typing import Any
from datetime import datetime

from blurr.core.field import FieldSchema


class IntegerFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return int

    @property
    def default(self) -> Any:
        return int(0)


class FloatFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return float

    @property
    def default(self) -> Any:
        return float(0)


class StringFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return str

    @property
    def default(self) -> Any:
        return str()


class BooleanFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return bool

    @property
    def default(self) -> Any:
        return False


class DateTimeFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return datetime

    @property
    def default(self) -> Any:
        return None


class MapFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return dict

    @property
    def default(self) -> Any:
        return dict()


class ListFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return list

    @property
    def default(self) -> Any:
        return list()


class SetFieldSchema(FieldSchema):

    @property
    def type_object(self) -> Any:
        return set

    @property
    def default(self) -> Any:
        return set()
