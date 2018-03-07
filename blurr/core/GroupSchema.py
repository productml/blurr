from core.FieldSchema import FieldSchema


class GroupSchema:
    def __init__(self, schema: dict) -> None:
        self.schema = schema
        self.name = schema['Name']
        self.type = schema['Type']
        self.filter = schema['Filter']
        self.fields = {s['Name']: FieldSchema(s) for s in schema['Fields']}
