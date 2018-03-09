from typing import Any

ITEM_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': 'blurr.core.session_data_group.SessionDataGroup'
}

SCHEMA_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': 'blurr.core.session_data_group.SessionDataGroupSchema',
    'string': 'blurr.core.field_types.StringType',
    'integer': 'blurr.core.field_types.IntegerType',
    'boolean': 'blurr.core.field_types.BooleanType',
    'datetime': 'blurr.core.field_types.DateTimeType',
    'float': 'blurr.core.field_types.FloatType',
    'map': 'blurr.core.field_types.MapType',
    'list': 'blurr.core.field_types.ListType',
    'set': 'blurr.core.field_types.SetType'
}


# TODO Build dynamic type loader from a central configuration rather than reading a static dictionary

class TypeLoader:

    @staticmethod
    def load_schema(type_name: str) -> Any:
        return TypeLoader._load_type(type_name, SCHEMA_MAP)

    @staticmethod
    def load_item(type_name: str) -> Any:
        return TypeLoader._load_type(type_name, ITEM_MAP)

    @staticmethod
    def _load_type(type_name: str, map: dict) -> Any:
        return TypeLoader._import_by_full_name(map[type_name])

    @staticmethod
    def _import_by_full_name(name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod
