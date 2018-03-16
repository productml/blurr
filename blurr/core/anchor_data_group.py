from typing import Dict, Any, Type

from blurr.core.base import BaseSchema
from blurr.core.data_group import DataGroup, DataGroupSchema
from blurr.core.evaluation import EvaluationContext
from blurr.core.loader import TypeLoader
from blurr.core.window import Window


class AnchorDataGroupSchema(DataGroupSchema):
    ATTRIBUTE_WINDOW = 'Window'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)

    def validate(self, spec: Dict[str, Any]):
        """
        Overrides the Base Schema validation specifications to include validation for nested schema
        """
        # Validate base attributes first
        super().validate(spec)

    def load(self, spec: Dict[str, Any]) -> None:
        """
        Overrides base load to include loads for nested items
        """
        # Loading the base attributes first
        super().load(spec)

        self.window_schema: Type[BaseSchema] = TypeLoader.load_schema(
            'window')(spec[self.ATTRIBUTE_WINDOW]
                      ) if self.ATTRIBUTE_WINDOW in spec else None


class AnchorDataGroup(DataGroup):
    def __init__(self, schema: AnchorDataGroupSchema,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, evaluation_context)
        self.window = Window(self.schema.window_schema
                             ) if schema.window_schema is not None else None
        if self.window is not None:
            self.evaluation_context.local_context.add(self.window.schema.name,
                                                      self.window)

    def prepare_window(self, store, identity, start_time):
        # evaluate window first which sets the correct window in the store
        if self.window is not None:
            self.window.prepare(store, identity, start_time)
