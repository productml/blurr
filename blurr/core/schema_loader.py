from typing import Dict, Any, List, Tuple, Optional

from blurr.core.errors import InvalidSchemaError
from blurr.core.loader import TypeLoader
from blurr.core.type import Type


class SchemaLoader:
    """
    Provides functionality to operate on the schema using fully qualified names.
    """
    ATTRIBUTE_NAME = 'Name'
    ATTRIBUTE_TYPE = 'Type'
    ITEM_SEPARATOR = '.'

    def __init__(self):
        self._spec = {}
        self._object_cache: Dict[str, 'BaseSchema'] = {}

    def add_schema(self, spec: Dict[str, Any],
                   fully_qualified_parent_name: str = None) -> Optional[str]:
        """
        Add a schema dictionary to the schema loader. The given schema is stored
        against fully_qualified_parent_name + ITEM_SEPARATOR('.') + schema.name.
        :param spec: Schema specification.
        :param fully_qualified_parent_name: Full qualified name of the parent.
        If None is passed then the schema is stored against the schema name.
        :return: The fully qualified name against which the spec is stored.
        None is returned if the given spec is not a dictionary or the spec does not
        contain a 'name' key.
        """
        if not isinstance(spec, dict) or self.ATTRIBUTE_NAME not in spec:
            return None

        name = spec[self.ATTRIBUTE_NAME]
        fully_qualified_name = name if fully_qualified_parent_name is None else self.get_fully_qualified_name(
            fully_qualified_parent_name, name)

        self._spec[fully_qualified_name] = spec
        for key, val in spec.items():
            if isinstance(val, list):
                for item in val:
                    self.add_schema(item, fully_qualified_name)
            self.add_schema(val, fully_qualified_name)

        return spec[self.ATTRIBUTE_NAME]

    # Using forward reference to avoid cyclic dependency.
    def get_schema_object(self, fully_qualified_name: str) -> 'BaseSchema':
        """
        Used to generate a schema object from the given fully_qualified_name.
        :param fully_qualified_name: The fully qualified name of the object needed.
        :return: An initialized schema object
        """

        if fully_qualified_name not in self._object_cache:
            spec = self.get_schema_spec(fully_qualified_name)
            if self.ATTRIBUTE_TYPE not in spec:
                raise InvalidSchemaError('Type not defined in schema')
            self._object_cache[fully_qualified_name] = TypeLoader.load_schema(
                spec[self.ATTRIBUTE_TYPE])(fully_qualified_name, self)

        return self._object_cache[fully_qualified_name]

    def get_nested_schema_object(self, fully_qualified_parent_name: str,
                                 nested_item_name: str) -> 'BaseSchema':
        """
        Used to generate a schema object from the given fully_qualified_parent_name
        and the nested_item_name.
        :param fully_qualified_parent_name: The fully qualified name of the parent.
        :param nested_item_name: The nested item name.
        :return: An initialized schema object of the nested item.
        """
        return self.get_schema_object(
            self.get_fully_qualified_name(fully_qualified_parent_name, nested_item_name))

    @staticmethod
    def get_fully_qualified_name(fully_qualified_parent_name: str, nested_item_name: str) -> str:
        """
        Returns the fully qualified name by combining the fully_qualified_parent_name
        and nested_item_name.
        :param fully_qualified_parent_name: The fully qualified name of the parent.
        :param nested_item_name: The nested item name.
        :return: The fully qualified name of the nested item.
        """
        return fully_qualified_parent_name + SchemaLoader.ITEM_SEPARATOR + nested_item_name

    def get_schema_spec(self, fully_qualified_name: str) -> Dict[str, Any]:
        """
        Used to retrieve the specifications of the schema from the given
        fully_qualified_name of schema.
        :param fully_qualified_name: The fully qualified name of the schema needed.
        :return: Schema dictionary.
        """
        try:
            return self._spec[fully_qualified_name]
        except:
            raise InvalidSchemaError("{} not declared in schema".format(fully_qualified_name))

    def get_schemas_of_type(self, schema_type: Type) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Returns a list of fully qualified names and schema dictionary tuples for
        the schema type provided.
        :param type: Schema type.
        :return: List of fully qualified names and schema dictionary tuples.
        """

        return [(fq_name, schema)
                for fq_name, schema in self._spec.items()
                if Type.is_type_equal(schema.get(self.ATTRIBUTE_TYPE, ''), schema_type)]

    @staticmethod
    def get_transformer_name(fully_qualified_name: str) -> str:
        """
        Returns the DTC transformer name based on the given fully_qualified_name
        of one of the nested child items.
        :param fully_qualified_name: Fully qualified name of the nested child item.
        :return: DTC transformer name.
        """
        return fully_qualified_name.split(SchemaLoader.ITEM_SEPARATOR, 1)[0]
