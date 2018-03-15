import importlib
from typing import Any

ITEM_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': 'blurr.core.session_data_group.SessionDataGroup',
    'string': 'blurr.core.simple_field.SimpleField',
    'integer': 'blurr.core.simple_field.SimpleField',
    'boolean': 'blurr.core.simple_field.SimpleField',
    'datetime': 'blurr.core.simple_field.SimpleField',
    'float': 'blurr.core.simple_field.SimpleField',
    'map': 'blurr.core.simple_field.SimpleField',
    'list': 'blurr.core.simple_field.SimpleField',
    'set': 'blurr.core.simple_field.SimpleField',
}

SCHEMA_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': 'blurr.core.session_data_group.SessionDataGroupSchema',
    'string': 'blurr.core.simple_field.StringFieldSchema',
    'integer': 'blurr.core.simple_field.IntegerFieldSchema',
    'boolean': 'blurr.core.simple_field.BooleanFieldSchema',
    'datetime': 'blurr.core.simple_field.DateTimeFieldSchema',
    'float': 'blurr.core.simple_field.FloatFieldSchema',
    'map': 'blurr.core.simple_field.MapFieldSchema',
    'list': 'blurr.core.simple_field.ListFieldSchema',
    'set': 'blurr.core.simple_field.SetFieldSchema'
}

STORE_MAP = {
    'ProductML:DTC:Store:MemoryStore': 'blurr.store.memory_store.MemoryStore'
}

# TODO Build dynamic type loader from a central configuration rather than reading a static dictionary


class TypeLoader:
    @staticmethod
    def load_schema(type_name: str):
        return TypeLoader.load_type(type_name, SCHEMA_MAP)

    @staticmethod
    def load_item(type_name: str):
        return TypeLoader.load_type(type_name, ITEM_MAP)

    @staticmethod
    def load_store(type_name: str):
        return TypeLoader.load_type(type_name, STORE_MAP)

    @staticmethod
    def load_type(type_name: str, type_map: dict) -> Any:
        return TypeLoader.import_class_by_full_name(type_map[type_name])

    @staticmethod
    def import_class_by_full_name(name):
        components = name.rsplit('.', 1)
        mod = importlib.import_module(components[0])
        loaded_class = getattr(mod, components[1])
        return loaded_class
