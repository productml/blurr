from typing import Dict, Any, Type

from blurr.core.base import BaseSchema
from blurr.core.data_group import DataGroup, DataGroupSchema
from blurr.core.evaluation import EvaluationContext
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.window import Window


class AnchorDataGroupSchema(DataGroupSchema):
    ATTRIBUTE_WINDOW = 'Window'

    def validate(self, spec: Dict[str, Any]):
        """
        Overrides the Base Schema validation specifications to include validation for nested schema
        """
        # Validate base attributes first
        super().validate(spec)

    def load(self) -> None:
        """
        Overrides base load to include loads for nested items
        """
        # Loading the base attributes first
        super().load()

        if self.ATTRIBUTE_WINDOW in self._spec:
            self.add_window_name()
            self.window_schema = self.schema_loader.get_schema_object(
                self.fully_qualified_name + '.' +
                self._spec[self.ATTRIBUTE_WINDOW][self.ATTRIBUTE_NAME])
        else:
            self.window_schema = None

    def add_window_name(self) -> None:
        if self.ATTRIBUTE_NAME not in self._spec[self.ATTRIBUTE_WINDOW]:
            self._spec[self.ATTRIBUTE_WINDOW][self.ATTRIBUTE_NAME] = 'source'
            self.schema_loader.add_schema(self._spec[self.ATTRIBUTE_WINDOW],
                                          self.fully_qualified_name)


class AnchorDataGroup(DataGroup):
    def __init__(self, schema: AnchorDataGroupSchema,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, evaluation_context)
        self.window = Window(self.schema.window_schema
                             ) if schema.window_schema is not None else None

    def prepare_window(self, store, identity, start_time):
        # evaluate window first which sets the correct window in the store
        if self.window is not None:
            self.window.prepare(store, identity, start_time)

    def evaluate(self) -> None:
        if self.window is not None:
            self.evaluation_context.local_context.add(self.window.schema.name,
                                                      self.window)
        super().evaluate()
