from core.BaseSchema import BaseSchema
from core.SessionGroupSchema import SessionGroupSchema

GROUP_TYPE_MAP = {
    'ProductML:DTC:DataGroup:SessionAggregate': SessionGroupSchema
}


class TransformerSchema(BaseSchema):
    def __init__(self, schema: dict) -> None:
        super().__init__(schema)
        self.version = schema['Version']
        self.description = schema['Description']
        self.identity = schema['Identity']
        self.time = schema['Time']
        # TODO Write factory for loading the correct group schema form different types
        self.groups = {s['Name']: self.load_group_schema(s) for s in schema['Groups']}

    def load_group_schema(self, schema):
        # TODO Move the type name to type reference out to an external configuration

        return GROUP_TYPE_MAP[schema['Type']](schema)
