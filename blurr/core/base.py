from abc import ABC, abstractmethod
from core.interpreter import Interpreter


class BaseSchema(ABC):
    def __init__(self, schema: dict):
        self.schema = schema
        self.name = schema['Name']
        self.type = schema['Type']
        self.filter = schema.get('Filter', None)
        self.filter_expr = None if self.filter is None else compile(self.filter, '<string>', 'eval')


class BaseItem(ABC):
    def __init__(self, schema: BaseSchema):
        self._schema = schema

    def should_evaluate(self, interpreter: Interpreter) -> None:
        return not self._schema.filter_expr or interpreter.evaluate(self._schema.filter_expr)

    @abstractmethod
    def evaluate(self, interpreter: Interpreter) -> None:
        return NotImplemented
