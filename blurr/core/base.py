from abc import ABC, abstractmethod
from core.interpreter import Interpreter


class BaseFieldNames:
    NAME = 'Name'
    TYPE = 'Type'
    FILTER = 'Filter'


class BaseSchema(ABC):
    def __init__(self, schema: dict):
        self.validate(schema)
        self.schema = schema
        self.name = schema[BaseFieldNames.NAME]
        self.type = schema[BaseFieldNames.TYPE]
        self.filter = schema.get(BaseFieldNames.FILTER, None)
        self.filter_expr = None if self.filter is None else compile(self.filter, '<string>', 'eval')

    @staticmethod
    def validate(schema):
        if BaseFieldNames.NAME not in schema:
            raise KeyError('{} is required for an item'.format(BaseFieldNames.NAME))

        if BaseFieldNames.TYPE not in schema:
            raise KeyError('{} is required for an item'.format(BaseFieldNames.NAME))


class BaseItem(ABC):
    def __init__(self, schema: BaseSchema):
        self._schema = schema

    def should_evaluate(self, interpreter: Interpreter) -> None:
        return not self._schema.filter_expr or interpreter.evaluate(self._schema.filter_expr)

    @abstractmethod
    def evaluate(self, interpreter: Interpreter) -> None:
        return NotImplemented
