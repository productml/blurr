from datetime import datetime, timedelta
from typing import Any, List, Tuple

from blurr.core.aggregate import Aggregate, AggregateSchema
from blurr.core.aggregate_block import BlockAggregate, BlockAggregateSchema
from blurr.core.errors import PrepareWindowMissingBlocksError
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.type import Type


class WindowAggregateSchema(AggregateSchema):

    ATTRIBUTE_WINDOW_VALUE = 'WindowValue'
    ATTRIBUTE_WINDOW_TYPE = 'WindowType'
    ATTRIBUTE_SOURCE = 'Source'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.window_value = self._spec.get(self.ATTRIBUTE_WINDOW_VALUE, 0)
        self.window_type = self._spec.get(self.ATTRIBUTE_WINDOW_TYPE, None)
        self.source: BlockAggregateSchema = self.schema_loader.get_schema_object(
            self._spec[self.ATTRIBUTE_SOURCE]) if self.ATTRIBUTE_SOURCE in self._spec else None

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_WINDOW_TYPE, self.ATTRIBUTE_WINDOW_VALUE,
                                          self.ATTRIBUTE_SOURCE)
        self.validate_number_attribute(self.ATTRIBUTE_WINDOW_VALUE, int)


class _WindowSource:
    """
    Represents a window on the pre-aggregated source data.
    """

    def __init__(self, block_list: List[BlockAggregate]):
        self.view: List[BlockAggregate] = block_list

    def __getattr__(self, item: str) -> List[Any]:
        return [getattr(block, item) for block in self.view]


class WindowAggregate(Aggregate):
    """
    Manages the generation of WindowAggregate as defined in the schema.
    """

    def __init__(self, schema: WindowAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)
        self._window_source = None

    def _prepare_window(self, start_time: datetime) -> None:
        """
        Prepares window if any is specified.
        :param start_time: The anchor block start_time from where the window
        should be generated.
        """
        # evaluate window first which sets the correct window in the store
        store = self._schema.schema_loader.get_store(
            self._schema.source.store_schema.fully_qualified_name)
        if Type.is_type_equal(self._schema.window_type, Type.DAY) or Type.is_type_equal(
                self._schema.window_type, Type.HOUR):
            block_list = self._load_blocks(
                store.get_range(
                    Key(self._identity, self._schema.source.name, start_time),
                    Key(self._identity, self._schema.source.name, self._get_end_time(start_time))))
        else:
            block_list = self._load_blocks(
                store.get_range(
                    Key(self._identity, self._schema.source.name, start_time), None,
                    self._schema.window_value))

        self._window_source = _WindowSource(block_list)
        self._validate_view()

    def _validate_view(self):
        if Type.is_type_equal(
                self._schema.window_type,
                Type.COUNT) and len(self._window_source.view) != abs(self._schema.window_value):
            raise PrepareWindowMissingBlocksError(
                '{} WindowAggregate: Expecting {} but found {} blocks'.format(
                    self._schema.name, abs(self._schema.window_value), len(
                        self._window_source.view)))

        if len(self._window_source.view) == 0:
            raise PrepareWindowMissingBlocksError(
                '{} WindowAggregate: No matching blocks found'.format(self._schema.name))

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
        if Type.is_type_equal(self._schema.window_type, Type.DAY):
            return start_time + timedelta(days=self._schema.window_value)
        elif Type.is_type_equal(self._schema.window_type, Type.HOUR):
            return start_time + timedelta(hours=self._schema.window_value)

    def _load_blocks(self, blocks: List[Tuple[Key, Any]]) -> List[BlockAggregate]:
        """
        Converts [(Key, block)] to [BlockAggregate]
        :param blocks: List of (Key, block) blocks.
        :return: List of BlockAggregate
        """
        return [
            BlockAggregate(self._schema.source, self._identity,
                           EvaluationContext()).run_restore(block) for (_, block) in blocks
        ]

    def run_evaluate(self) -> None:
        self._evaluation_context.local_context.add('source', self._window_source)
        super().run_evaluate()
