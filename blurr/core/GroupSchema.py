from core.FieldSchema import FieldSchema
from core.BaseSchema import BaseSchema


class GroupSchema(BaseSchema):
    def __init__(self, schema: dict) -> None:
        super().__init__(schema)
        self.fields = {s['Name']: FieldSchema(s) for s in schema['Fields']}
