from datetime import datetime
from typing import Dict, Any

from blurr.core.data_group import DataGroup, DataGroupSchema
from blurr.core.evaluation import EvaluationContext
from blurr.core.store import Store
from blurr.core.window import Window


class AnchorDataGroupSchema(DataGroupSchema):
    """
    Schema for AnchorAggregate DataGroup.
    """
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
    """
    Manages the generation of AnchorAggregate as defined in the schema.
    """

    def __init__(self, schema: AnchorDataGroupSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)
        self.window = Window(self.schema.window_schema
                             ) if schema.window_schema is not None else None

    def prepare_window(self, start_time: datetime) -> None:
        """
        Prepares window if any is specified.
        :param store: Store to be used to query for the source data.
        :param identity: Identity is used as a Key for store query.
        :param start_time: The Anchor session start_time from where the window
        should be generated.
        """
        # evaluate window first which sets the correct window in the store
        if self.window is not None:
            self.window.prepare(self.identity, start_time)

    def evaluate(self) -> None:
        if self.window is not None:
            self.evaluation_context.local_context.add(self.window.schema.name,
                                                      self.window)
        super().evaluate()