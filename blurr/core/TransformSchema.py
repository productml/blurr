from core.GroupSchema import GroupSchema


class TransformSchema:
    def __init__(self, schema: dict) -> None:
        self.schema = schema
        self.type = schema['Type']
        self.type = schema['Version']
        self.filter = schema['Filter']
        self.name = schema['Name']
        self.description = schema['Description']
        self.identity = schema['Identity']
        self.time = schema['Time']
        # TODO Write factory for loading the correct group schema form different types
        self.groups = {s['Name']: GroupSchema(s) for s in schema['Groups']}
