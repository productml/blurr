from abc import ABC, abstractmethod
from core.BaseSchema import BaseSchema
from core.Interpreter import Interpreter


class BaseItem(ABC):
    def __init__(self, schema: BaseSchema):
        self._schema = schema

    def should_evaluate(self, interpreter: Interpreter) -> None:
        return not self._schema.filter_expr or interpreter.evaluate(self._schema.filter_expr)

    @abstractmethod
    def evaluate(self, interpreter: Interpreter) -> None:
        return NotImplemented
