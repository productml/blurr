from datetime import datetime
from typing import Any

from dateutil import parser

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

    @staticmethod
    def encoder(value: Any) -> str:
        return value.isoformat() if value else None

    @staticmethod
    def decoder(value: Any) -> datetime:
        return parser.parse(value) if value else None
