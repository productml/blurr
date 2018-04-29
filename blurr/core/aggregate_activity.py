from datetime import timedelta

from blurr.core.aggregate import AggregateSchema
from blurr.core.aggregate_block import BlockAggregate, BlockAggregateSchema
from blurr.core.schema_loader import SchemaLoader


class ActivityAggregateSchema(BlockAggregateSchema):
    """ Aggregates activity into blocks by partitioning around periods of inactivity """

    ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS = 'SeparateByInactiveSeconds'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        try:
            value = int(self._spec[self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS])
            self.separation_interval: timedelta = timedelta(seconds=value)
        except:
            self.separation_interval: timedelta = timedelta()

    def validate_schema_spec(self) -> None:
        super(BlockAggregateSchema, self).validate_schema_spec()
        self.validate_required(self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS)
        self.validate_number(self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS, int, 1, None)


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
