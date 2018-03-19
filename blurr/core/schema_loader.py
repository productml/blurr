from typing import Dict, Any

from blurr.core.errors import InvalidSchemaError
from blurr.core.loader import TypeLoader


class SchemaLoader:
    ATTRIBUTE_NAME = 'Name'
    ATTRIBUTE_TYPE = 'Type'

    def __init__(self):
        self._spec = {}

    def add_schema(self, spec: Dict[str, Any],
                   full_parent_name: str = None) -> str:
        if not isinstance(spec, dict):
            return None

        if self.ATTRIBUTE_NAME not in spec:
            return None

        name = spec[self.ATTRIBUTE_NAME]
        full_name = name if full_parent_name is None else full_parent_name + '.' + name

        self._spec[full_name] = spec
        for key, val in spec.items():
            if isinstance(val, list):
                for item in val:
                    self.add_schema(item, full_name)
            self.add_schema(val, full_name)

        return spec[self.ATTRIBUTE_NAME]

    def get_schema_object(self, schema_path: str):
        spec = self.get_schema_spec(schema_path)
        if self.ATTRIBUTE_TYPE not in spec:
            raise InvalidSchemaError('Name not defined in schema')
        return TypeLoader.load_schema(spec[self.ATTRIBUTE_TYPE])(schema_path,
                                                                 self)

    def get_schema_spec(self, schema_path: str) -> Dict[str, Any]:
        return self._spec[schema_path]

    def _get_schema(self, schema_path: str,
                    spec: Dict[str, Any]) -> Dict[str, Any]:
        schema_path_splits = schema_path.split('.', 1)
        if len(schema_path_splits) > 1:
            return self._get_schema(schema_path_splits[1],
                                    spec[schema_path_splits[0]])
        else:
            return spec[schema_path_splits[0]]
