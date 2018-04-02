from typing import Any

import importlib

from blurr.core.errors import InvalidSchemaError

ITEM_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': 'blurr.core.session_data_group.SessionDataGroup',
    'ProductML:DTC:DataGroup:IdentityAggregate': 'blurr.core.identity_data_group.IdentityDataGroup',
    'ProductML:DTC:DataGroup:VariableAggregate': 'blurr.core.variable_data_group.VariableDataGroup',
    'ProductML:DTC:DataGroup:AnchorAggregate': 'blurr.core.anchor_data_group.AnchorDataGroup',
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

SCHEMA_MAP = {
    'ProductML:DTC:Streaming': 'blurr.core.streaming_transformer.StreamingTransformerSchema',
    'ProductML:DTC:Window': 'blurr.core.window_transformer.WindowTransformerSchema',
    'ProductML:DTC:DataGroup:SessionAggregate': 'blurr.core.session_data_group.SessionDataGroupSchema',
    'ProductML:DTC:DataGroup:IdentityAggregate': 'blurr.core.identity_data_group.IdentityDataGroupSchema',
    'ProductML:DTC:DataGroup:VariableAggregate': 'blurr.core.variable_data_group.VariableDataGroupSchema',
    'ProductML:DTC:DataGroup:AnchorAggregate': 'blurr.core.anchor_data_group.AnchorDataGroupSchema',
    'ProductML:DTC:Store:MemoryStore': 'blurr.store.memory_store.MemoryStore',
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

# TODO Build dynamic type loader from a central configuration rather than reading a static dictionary


class TypeLoader:
    @staticmethod
    def load_schema(type_name: str):
        return TypeLoader.load_type(type_name, SCHEMA_MAP)

    @staticmethod
    def load_item(type_name: str):
        return TypeLoader.load_type(type_name, ITEM_MAP)

    @staticmethod
    def load_type(type_name: str, type_map: dict) -> Any:
        if type_name not in type_map:
            raise InvalidSchemaError(
                'Unknown schema type {}'.format(type_name))
        return TypeLoader.import_class_by_full_name(type_map[type_name])

    @staticmethod
    def import_class_by_full_name(name):
        components = name.rsplit('.', 1)
        mod = importlib.import_module(components[0])
        loaded_class = getattr(mod, components[1])
        return loaded_class
