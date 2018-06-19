from datetime import datetime, timezone
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
    def sanitize_object(instance: datetime) -> datetime:
        if instance.tzinfo is None or instance.tzinfo.utcoffset(instance) is None:
            instance = instance.replace(tzinfo=timezone.utc)

        return instance

    @staticmethod
    def encoder(value: Any) -> str:
        return value.isoformat() if value else None

    def decoder(self, value: Any) -> datetime:
        return self.sanitize_object(parser.parse(value)) if value else None
