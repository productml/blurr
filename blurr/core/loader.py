from typing import Any
from blurr.core.session_group import SessionGroupSchema, SessionGroup
from blurr.core.errors import TypeNotFoundException
from blurr.core.field_types import IntegerType, StringType, BooleanType, DateTimeType, FloatType, MapType, ListType, \
    SetType

ITEM_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': SessionGroup
}

SCHEMA_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': SessionGroupSchema,
    'string': StringType,
    'integer': IntegerType,
    'boolean': BooleanType,
    'datetime': DateTimeType,
    'float': FloatType,
    'map': MapType,
    'list': ListType,
    'set': SetType
}


class TypeLoader:

    @staticmethod
    def load_type(type_name: str, schema=True) -> Any:
        if (schema and type_name not in SCHEMA_MAP) or (
                not schema and type_name not in ITEM_MAP):
            raise TypeNotFoundException('Type "{}" not found in type directory.', type_name)
        return SCHEMA_MAP[type_name] if schema else ITEM_MAP[type_name]
