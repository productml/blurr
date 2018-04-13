from typing import Any

import importlib

from blurr.core.errors import InvalidSchemaError

ITEM_MAP = {
    'Blurr:Transform:Streaming': 'blurr.core.transformer_streaming.StreamingTransformer',
    'Blurr:Transform:Window': 'blurr.core.transformer_window.WindowTransformer',
    'Blurr:Aggregate:BlockAggregate': 'blurr.core.aggregate_block.BlockAggregate',
    'Blurr:Aggregate:IdentityAggregate': 'blurr.core.aggregate_identity.IdentityAggregate',
    'Blurr:Aggregate:VariableAggregate': 'blurr.core.aggregate_variable.VariableAggregate',
    'Blurr:Aggregate:WindowAggregate': 'blurr.core.aggregate_window.WindowAggregate',
    'day': 'blurr.core.window.Window',
    'hour': 'blurr.core.window.Window',
    'count': 'blurr.core.window.Window',
    'string': 'blurr.core.field_simple.SimpleField',
    'integer': 'blurr.core.field_simple.SimpleField',
    'boolean': 'blurr.core.field_simple.SimpleField',
    'datetime': 'blurr.core.field_simple.SimpleField',
    'float': 'blurr.core.field_simple.SimpleField',
    'map': 'blurr.core.field_simple.SimpleField',
    'list': 'blurr.core.field_simple.SimpleField',
    'set': 'blurr.core.field_simple.SimpleField',
}
ITEM_MAP_LOWER_CASE = {k.lower(): v for k, v in ITEM_MAP.items()}

SCHEMA_MAP = {
    'Blurr:Transform:Streaming': 'blurr.core.transformer_streaming.StreamingTransformerSchema',
    'Blurr:Transform:Window': 'blurr.core.transformer_window.WindowTransformerSchema',
    'Blurr:Aggregate:BlockAggregate': 'blurr.core.aggregate_block.BlockAggregateSchema',
    'Blurr:Aggregate:IdentityAggregate': 'blurr.core.aggregate_identity.IdentityAggregateSchema',
    'Blurr:Aggregate:VariableAggregate': 'blurr.core.aggregate_variable.VariableAggregateSchema',
    'Blurr:Aggregate:WindowAggregate': 'blurr.core.aggregate_window.WindowAggregateSchema',
    'Blurr:Store:MemoryStore': 'blurr.store.memory_store.MemoryStore',
    'anchor': 'blurr.core.anchor.AnchorSchema',
    'day': 'blurr.core.window.WindowSchema',
    'hour': 'blurr.core.window.WindowSchema',
    'count': 'blurr.core.window.WindowSchema',
    'string': 'blurr.core.field_simple.StringFieldSchema',
    'integer': 'blurr.core.field_simple.IntegerFieldSchema',
    'boolean': 'blurr.core.field_simple.BooleanFieldSchema',
    'datetime': 'blurr.core.field_simple.DateTimeFieldSchema',
    'float': 'blurr.core.field_simple.FloatFieldSchema',
    'map': 'blurr.core.field_complex.MapFieldSchema',
    'list': 'blurr.core.field_complex.ListFieldSchema',
    'set': 'blurr.core.field_complex.SetFieldSchema'
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
