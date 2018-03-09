from typing import Any

from blurr.core.errors import TypeNotFoundException

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
    def load_type(type_name: str, schema=True) -> Any:
        if (schema and type_name not in SCHEMA_MAP) or (
                not schema and type_name not in ITEM_MAP):
            raise TypeNotFoundException('Type "{}" not found in type directory.', type_name)
        return TypeLoader.import_by_full_name(SCHEMA_MAP[type_name] if schema else ITEM_MAP[type_name])

    @staticmethod
    def import_by_full_name(name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod
