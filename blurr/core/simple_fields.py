from datetime import datetime
from typing import Any

from blurr.core.field import FieldSchema, Field


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


class SimpleField(Field):
    """
    Represents a simple field that can be of any native feild type
    """
    pass
