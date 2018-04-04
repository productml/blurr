from typing import Any

import importlib

from blurr.core.errors import InvalidSchemaError

ITEM_MAP = {
    'Blurr:DataGroup:BlockAggregate': 'blurr.core.block_data_group.BlockDataGroup',
    'Blurr:DataGroup:IdentityAggregate': 'blurr.core.identity_data_group.IdentityDataGroup',
    'Blurr:DataGroup:VariableAggregate': 'blurr.core.variable_data_group.VariableDataGroup',
    'Blurr:DataGroup:WindowAggregate': 'blurr.core.window_data_group.WindowDataGroup',
    'day': 'blurr.core.window.Window',
    'hour': 'blurr.core.window.Window',
    'count': 'blurr.core.window.Window',
    'string': 'blurr.core.simple_fields.SimpleField',
    'integer': 'blurr.core.simple_fields.SimpleField',
    'boolean': 'blurr.core.simple_fields.SimpleField',
    'datetime': 'blurr.core.simple_fields.SimpleField',
    'float': 'blurr.core.simple_fields.SimpleField',
    'map': 'blurr.core.simple_fields.SimpleField',
    'list': 'blurr.core.simple_fields.SimpleField',
    'set': 'blurr.core.simple_fields.SimpleField',
}
ITEM_MAP_LOWER_CASE = {k.lower(): v for k, v in ITEM_MAP.items()}

SCHEMA_MAP = {
    'Blurr:Streaming': 'blurr.core.streaming_transformer.StreamingTransformerSchema',
    'Blurr:Window': 'blurr.core.window_transformer.WindowTransformerSchema',
    'Blurr:DataGroup:BlockAggregate': 'blurr.core.block_data_group.BlockDataGroupSchema',
    'Blurr:DataGroup:IdentityAggregate': 'blurr.core.identity_data_group.IdentityDataGroupSchema',
    'Blurr:DataGroup:VariableAggregate': 'blurr.core.variable_data_group.VariableDataGroupSchema',
    'Blurr:DataGroup:WindowAggregate': 'blurr.core.window_data_group.WindowDataGroupSchema',
    'Blurr:Store:MemoryStore': 'blurr.store.memory_store.MemoryStore',
    'anchor': 'blurr.core.anchor.AnchorSchema',
    'day': 'blurr.core.window.WindowSchema',
    'hour': 'blurr.core.window.WindowSchema',
    'count': 'blurr.core.window.WindowSchema',
    'string': 'blurr.core.simple_fields.StringFieldSchema',
    'integer': 'blurr.core.simple_fields.IntegerFieldSchema',
    'boolean': 'blurr.core.simple_fields.BooleanFieldSchema',
    'datetime': 'blurr.core.simple_fields.DateTimeFieldSchema',
    'float': 'blurr.core.simple_fields.FloatFieldSchema',
    'map': 'blurr.core.complex_fields.MapFieldSchema',
    'list': 'blurr.core.complex_fields.ListFieldSchema',
    'set': 'blurr.core.complex_fields.SetFieldSchema'
}
SCHEMA_MAP_LOWER_CASE = {k.lower(): v for k, v in SCHEMA_MAP.items()}

# TODO Build dynamic type loader from a central configuration rather than reading a static dictionary


class TypeLoader:
    @staticmethod
    def load_schema(type_name: str):
        return TypeLoader.load_type(type_name, SCHEMA_MAP_LOWER_CASE)

    @staticmethod
    def load_item(type_name: str):
        return TypeLoader.load_type(type_name, ITEM_MAP_LOWER_CASE)

    @staticmethod
    def load_type(type_name: str, type_map: dict) -> Any:
        lower_type_name = type_name.lower()
        if lower_type_name not in type_map:
            raise InvalidSchemaError('Unknown schema type {}'.format(type_name))
        return TypeLoader.import_class_by_full_name(type_map[lower_type_name])

    @staticmethod
    def import_class_by_full_name(name):
        components = name.rsplit('.', 1)
        mod = importlib.import_module(components[0])
        loaded_class = getattr(mod, components[1])
        return loaded_class
