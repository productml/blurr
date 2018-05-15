import importlib
from typing import Any, Union, Dict, Optional

from blurr.core.errors import GenericSchemaError
from blurr.core.type import Type

ITEM_MAP = {
    Type.BLURR_TRANSFORM_STREAMING: 'blurr.core.transformer_streaming.StreamingTransformer',
    Type.BLURR_TRANSFORM_WINDOW: 'blurr.core.transformer_window.WindowTransformer',
    Type.BLURR_AGGREGATE_BLOCK: 'blurr.core.aggregate_block.BlockAggregate',
    Type.BLURR_AGGREGATE_ACTIVITY: 'blurr.core.aggregate_activity.ActivityAggregate',
    Type.BLURR_AGGREGATE_IDENTITY: 'blurr.core.aggregate_identity.IdentityAggregate',
    Type.BLURR_AGGREGATE_VARIABLE: 'blurr.core.aggregate_variable.VariableAggregate',
    Type.BLURR_AGGREGATE_WINDOW: 'blurr.core.aggregate_window.WindowAggregate',
    Type.BLURR_STORE_MEMORY: 'blurr.store.memory_store.MemoryStore',
    Type.BLURR_STORE_DYNAMO: 'blurr.store.dynamo_store.DynamoStore',
    Type.DAY: 'blurr.core.window.Window',
    Type.HOUR: 'blurr.core.window.Window',
    Type.COUNT: 'blurr.core.window.Window',
    Type.STRING: 'blurr.core.field.Field',
    Type.INTEGER: 'blurr.core.field.Field',
    Type.BOOLEAN: 'blurr.core.field.Field',
    Type.DATETIME: 'blurr.core.field.Field',
    Type.FLOAT: 'blurr.core.field.Field',
    Type.MAP: 'blurr.core.field.Field',
    Type.LIST: 'blurr.core.field.Field',
    Type.SET: 'blurr.core.field.Field',
}

SCHEMA_MAP = {
    Type.BLURR_TRANSFORM_STREAMING: 'blurr.core.transformer_streaming.StreamingTransformerSchema',
    Type.BLURR_TRANSFORM_WINDOW: 'blurr.core.transformer_window.WindowTransformerSchema',
    Type.BLURR_AGGREGATE_BLOCK: 'blurr.core.aggregate_block.BlockAggregateSchema',
    Type.BLURR_AGGREGATE_ACTIVITY: 'blurr.core.aggregate_activity.ActivityAggregateSchema',
    Type.BLURR_AGGREGATE_IDENTITY: 'blurr.core.aggregate_identity.IdentityAggregateSchema',
    Type.BLURR_AGGREGATE_VARIABLE: 'blurr.core.aggregate_variable.VariableAggregateSchema',
    Type.BLURR_AGGREGATE_WINDOW: 'blurr.core.aggregate_window.WindowAggregateSchema',
    Type.BLURR_STORE_MEMORY: 'blurr.store.memory_store.MemoryStoreSchema',
    Type.BLURR_STORE_DYNAMO: 'blurr.store.dynamo_store.DynamoStoreSchema',
    Type.ANCHOR: 'blurr.core.anchor.AnchorSchema',
    Type.DAY: 'blurr.core.window.WindowSchema',
    Type.HOUR: 'blurr.core.window.WindowSchema',
    Type.COUNT: 'blurr.core.window.WindowSchema',
    Type.STRING: 'blurr.core.field_simple.StringFieldSchema',
    Type.INTEGER: 'blurr.core.field_simple.IntegerFieldSchema',
    Type.BOOLEAN: 'blurr.core.field_simple.BooleanFieldSchema',
    Type.DATETIME: 'blurr.core.field_simple.DateTimeFieldSchema',
    Type.FLOAT: 'blurr.core.field_simple.FloatFieldSchema',
    Type.MAP: 'blurr.core.field_complex.MapFieldSchema',
    Type.LIST: 'blurr.core.field_complex.ListFieldSchema',
    Type.SET: 'blurr.core.field_complex.SetFieldSchema'
}

# TODO Build dynamic type loader from a central configuration rather than reading a static dictionary

_class_cache: Dict[str, Type] = {}

class TypeLoader:
    @staticmethod
    def load_schema(type_name: Union[str, Type]):
        return TypeLoader.load_type(type_name, SCHEMA_MAP)

    @staticmethod
    def load_item(type_name: Union[str, Type]):
        return TypeLoader.load_type(type_name, ITEM_MAP)

    @staticmethod
    def load_type(type_name: Union[str, Type], type_map: dict) -> Any:
        try:
            type_name_enum = Type(type_name)
            return TypeLoader.import_class_by_full_name(type_map[type_name_enum])
        except (KeyError, ValueError):
            raise GenericSchemaError('Type `{}` not found.'.format(type_name))

    @staticmethod
    def import_class_by_full_name(name) -> Optional[Type]:
        if name not in _class_cache:
            components = name.rsplit('.', 1)
            mod = importlib.import_module(components[0])
            _class_cache[name] = mod if len(components) == 1 else getattr(mod, components[1])
        return _class_cache.get(name, None)
