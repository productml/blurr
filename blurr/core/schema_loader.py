from typing import Dict, Any

from blurr.core.errors import InvalidSchemaError
from blurr.core.loader import TypeLoader


class SchemaLoader:
    """
    Provides functionality to operate on the schema using fully qualified names.
    """
    ATTRIBUTE_NAME = 'Name'
    ATTRIBUTE_TYPE = 'Type'

    def __init__(self):
        self._spec = {}

    def add_schema(self, spec: Dict[str, Any],
                   fully_qualified_parent_name: str = None) -> str:
        if not isinstance(spec, dict):
            return None

        if self.ATTRIBUTE_NAME not in spec:
            return None

        name = spec[self.ATTRIBUTE_NAME]
        fully_qualified_name = name if fully_qualified_parent_name is None else fully_qualified_parent_name + '.' + name

        self._spec[fully_qualified_name] = spec
        for key, val in spec.items():
            if isinstance(val, list):
                for item in val:
                    self.add_schema(item, fully_qualified_name)
            self.add_schema(val, fully_qualified_name)

        return spec[self.ATTRIBUTE_NAME]

    def get_schema_object(self, fully_qualified_name: str):
        spec = self.get_schema_spec(fully_qualified_name)
        if self.ATTRIBUTE_TYPE not in spec:
            raise InvalidSchemaError('Name not defined in schema')
        return TypeLoader.load_schema(spec[self.ATTRIBUTE_TYPE])(fully_qualified_name,
                                                                 self)

    def get_schema_spec(self, fully_qualified_name: str) -> Dict[str, Any]:
        return self._spec[fully_qualified_name]
