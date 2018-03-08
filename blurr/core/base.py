from abc import ABC, abstractmethod
from blurr.core.context import Context
import dis


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
        self.filter_expr = None if self.filter is None else compile(
            self.filter, '<string>', 'eval')

    @staticmethod
    def validate(schema):
        if BaseFieldNames.NAME not in schema:
            raise KeyError('{} is required for an item'.format(
                BaseFieldNames.NAME))

        if BaseFieldNames.TYPE not in schema:
            raise KeyError('{} is required for an item'.format(
                BaseFieldNames.NAME))


class BaseItem(ABC):
    def __init__(self, schema: BaseSchema, global_context: Context=Context(), local_context: Context=Context()):
        self._schema = schema
        self._global_context = global_context
        self._local_context = local_context

    def evaluate_expr(self, expr):
        try:
            return eval(expr, self._global_context, self._local_context)
        except Exception as e:
            print(dis.dis(expr))
            raise e

    def should_evaluate(self) -> None:
        return not self._schema.filter_expr or self.evaluate_expr(self._schema.filter_expr)

    @abstractmethod
    def evaluate(self) -> None:
        return NotImplemented
