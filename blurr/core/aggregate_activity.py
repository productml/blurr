from datetime import timedelta

from blurr.core.aggregate import AggregateSchema
from blurr.core.aggregate_block import BlockAggregate, BlockAggregateSchema
from blurr.core.errors import SchemaErrorCollection
from blurr.core.schema_loader import SchemaLoader


class ActivityAggregateSchema(BlockAggregateSchema):
    """ Aggregates activity into blocks by partitioning around periods of inactivity """

    ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS = 'SeparateByInactiveSeconds'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.separation_interval: timedelta = timedelta(
            seconds=int(self._spec[
                            self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS])) if self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS in self._spec else None

    def validate_schema_spec(self) -> None:
        super(AggregateSchema, self).validate_schema_spec()
        self.validate_required(self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS)


class ActivityAggregate(BlockAggregate):
    """ Aggregates activity in blocks separated by periods of inactivity"""

    def evaluate(self) -> None:
        time = self._evaluation_context.global_context['time']

        # If the event time is beyond separation threshold, create a new block
        if self._start_time and (time < self._start_time - self._schema.separation_interval
                                 or time > self._end_time + self._schema.separation_interval):
            self.persist(self._start_time)
            self.reset()

        super().evaluate()

    def finalize(self):
        self.persist(self._start_time)
