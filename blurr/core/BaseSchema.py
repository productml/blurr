from abc import ABC


class BaseSchema(ABC):
    def __init__(self, schema: dict):
        self.schema = schema
        self.name = schema['Name']
        self.type = schema['Type']
        self.filter = schema.get('Filter', '')
        self.filter_expr = compile(self.filter, '<string>', 'eval')