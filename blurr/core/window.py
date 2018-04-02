from datetime import datetime, timedelta
from typing import Any, List, Tuple

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.errors import PrepareWindowMissingBlocksError
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.block_data_group import BlockDataGroup
from blurr.core.store import Key


class WindowSchema(BaseSchema):
    """
    Represents the schema for the window to be created on the pre-aggregated
    source data.
    """
    ATTRIBUTE_VALUE = 'Value'
    ATTRIBUTE_SOURCE = 'Source'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.value = self._spec[self.ATTRIBUTE_VALUE]
        self.source = self.schema_loader.get_schema_object(
            self._spec[self.ATTRIBUTE_SOURCE])


class Window:
    """
    Generates a window view on the pre-aggregated source data.
    Does not inherit from BaseItem as a window is used for setting up the evaluation
    context and does not directly participate in the evaluation path.
    """

    def __init__(self, schema: WindowSchema) -> None:
        self.schema = schema
        self.view: List[BlockDataGroup] = []

    def prepare(self, identity: str, start_time: datetime) -> None:
        """
        Prepares the window view on the source data.
        :param store: Store to be used to query for the source data.
        :param identity: Identity is used as a Key for store query.
        :param start_time: The Anchor block start_time from where the window
        should be generated.
        :return: None
        """
        store = self.schema.source.store
        if self.schema.type == 'day' or self.schema.type == 'hour':
            self.view = self._load_blocks(
                store.get_range(
                    Key(identity, self.schema.source.name, start_time),
                    Key(identity, self.schema.source.name,
                        self._get_end_time(start_time))), identity)
        else:
            self.view = self._load_blocks(
                store.get_range(
                    Key(identity, self.schema.source.name, start_time), None,
                    self.schema.value), identity)

        self._validate_view()

    def _validate_view(self):
        if self.schema.type == 'count' and len(self.view) != abs(
                self.schema.value):
            raise PrepareWindowMissingBlocksError(
                'Expecting {} but not found {} blocks'.format(
                    abs(self.schema.value), len(self.view)))

        if len(self.view) == 0:
            raise PrepareWindowMissingBlocksError('No matching blocks found')

    # TODO: Handle end time which is beyond the expected range of data being
    # processed. In this case a PrepareWindowMissingBlocksError error should
    # be raised.
    def _get_end_time(self, start_time: datetime) -> datetime:
        """
        Generates the end time to be used for the store range query.
        :param start_time: Start time to use as an offset to calculate the end time
        based on the window schema.
        :return:
        """
        if self.schema.type == 'day':
            return start_time + timedelta(days=self.schema.value)
        elif self.schema.type == 'hour':
            return start_time + timedelta(hours=self.schema.value)

    def _load_blocks(self, blocks: List[Tuple[Key, Any]],
                     identity: str) -> List[BaseItem]:
        """
        Converts [(Key, Block)] to [BlockDataGroup]
        :param blocks: List of (Key, Block) blocks.
        :return: List of BlockDataGroup
        """
        return [
            BlockDataGroup(self.schema.source, identity,
                           EvaluationContext()).restore(block)
            for (_, block) in blocks
        ]

    def __getattr__(self, item: str) -> List[Any]:
        return [getattr(block, item) for block in self.view]
