from datetime import datetime, timedelta
from typing import Any, List, Tuple

from blurr.core.data_group import DataGroup, DataGroupSchema
from blurr.core.errors import PrepareWindowMissingBlocksError
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.block_data_group import BlockDataGroup
from blurr.core.store import Key
from blurr.core.base import BaseItem


class WindowDataGroupSchema(DataGroupSchema):
    """
    Schema for WindowAggregate DataGroup.
    """
    ATTRIBUTE_WINDOW_VALUE = 'WindowValue'
    ATTRIBUTE_WINDOW_TYPE = 'WindowType'
    ATTRIBUTE_SOURCE = 'Source'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)
        self.window_value = self._spec[self.ATTRIBUTE_WINDOW_VALUE]
        self.window_type = self._spec[self.ATTRIBUTE_WINDOW_TYPE]
        self.source = self.schema_loader.get_schema_object(
            self._spec[self.ATTRIBUTE_SOURCE])


class _WindowSource:
    """
    Represents a window on the pre-aggregated source data.
    """

    def __init__(self):
        self.view: List[BlockDataGroup] = []

    def __getattr__(self, item: str) -> List[Any]:
        return [getattr(block, item) for block in self.view]


class WindowDataGroup(DataGroup):
    """
    Manages the generation of WindowAggregate as defined in the schema.
    """

    def __init__(self, schema: WindowDataGroupSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)
        self.window_source = _WindowSource()

    def prepare_window(self, start_time: datetime) -> None:
        """
        Prepares window if any is specified.
        :param start_time: The anchor block start_time from where the window
        should be generated.
        """
        # evaluate window first which sets the correct window in the store
        store = self.schema.source.store
        if self.schema.window_type == 'day' or self.schema.window_type == 'hour':
            self.window_source.view = self._load_blocks(
                store.get_range(
                    Key(self.identity, self.schema.source.name, start_time),
                    Key(self.identity, self.schema.source.name,
                        self._get_end_time(start_time))))
        else:
            self.window_source.view = self._load_blocks(
                store.get_range(
                    Key(self.identity, self.schema.source.name, start_time),
                    None, self.schema.window_value))

        self._validate_view()

    def _validate_view(self):
        if self.schema.window_type == 'count' and len(
                self.window_source.view) != abs(self.schema.window_value):
            raise PrepareWindowMissingBlocksError(
                'Expecting {} but not found {} blocks'.format(
                    abs(self.schema.window_value),
                    len(self.window_source.view)))

        if len(self.window_source.view) == 0:
            raise PrepareWindowMissingBlocksError('No matching blocks found')

    # TODO: Handle end time which is beyond the expected range of data being
    # processed. In this case a PrepareWindowMissingBlocksError error should
    # be raised.
    def _get_end_time(self, start_time: datetime) -> datetime:
        """
        Generates the end time to be used for the store range query.
        :param start_time: Start time to use as an offset to calculate the end time
        based on the window type in the schema.
        :return:
        """
        if self.schema.window_type == 'day':
            return start_time + timedelta(days=self.schema.window_value)
        elif self.schema.window_type == 'hour':
            return start_time + timedelta(hours=self.schema.window_value)

    def _load_blocks(self, blocks: List[Tuple[Key, Any]]) -> List[BaseItem]:
        """
        Converts [(Key, block)] to [BlockDataGroup]
        :param blocks: List of (Key, block) blocks.
        :return: List of BlockDataGroup
        """
        return [
            BlockDataGroup(self.schema.source, self.identity,
                           EvaluationContext()).restore(block)
            for (_, block) in blocks
        ]

    def evaluate(self) -> None:
        self.evaluation_context.local_context.add('source', self.window_source)
        super().evaluate()
