import importlib
from typing import Any, Dict
from blurr.core.base import BaseSchema, BaseItem

ITEM_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': 'blurr.core.session_data_group.SessionDataGroup',
    'string': 'blurr.core.field.Field',
    'integer': 'blurr.core.field.Field',
    'boolean': 'blurr.core.field.Field',
    'datetime': 'blurr.core.field.Field',
    'float': 'blurr.core.field.Field',
    'map': 'blurr.core.field.Field',
    'list': 'blurr.core.field.Field',
    'set': 'blurr.core.field.Field',
}

SCHEMA_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': 'blurr.core.session_data_group.SessionDataGroupSchema',
    'string': 'blurr.core.field_schema.StringFieldSchema',
    'integer': 'blurr.core.field_schema.IntegerFieldSchema',
    'boolean': 'blurr.core.field_schema.BooleanFieldSchema',
    'datetime': 'blurr.core.field_schema.DateTimeFieldSchema',
    'float': 'blurr.core.field_schema.FloatFieldSchema',
    'map': 'blurr.core.field_schema.MapFieldSchema',
    'list': 'blurr.core.field_schema.ListFieldSchema',
    'set': 'blurr.core.field_schema.SetFieldSchema'
}


# TODO Build dynamic type loader from a central configuration rather than reading a static dictionary

class TypeLoader:

    @staticmethod
    def load_schema(type_name: str) -> BaseSchema:
        return TypeLoader.load_type(type_name, SCHEMA_MAP)

    @staticmethod
    def load_item(type_name: str) -> BaseItem:
        return TypeLoader.load_type(type_name, ITEM_MAP)

    @staticmethod
    def load_type(type_name: str, map: dict) -> Any:
        return TypeLoader.import_class_by_full_name(map[type_name])

    @staticmethod
    def import_class_by_full_name(name):
        components = name.rsplit('.', 1)
        mod = importlib.import_module(components[0])
        loaded_class = getattr(mod, components[1])
        return loaded_class
