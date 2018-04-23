from datetime import timedelta

from blurr.core.aggregate_streaming import StreamingAggregate, StreamingAggregateSchema
from blurr.core.schema_loader import SchemaLoader


class ActivityAggregateSchema(StreamingAggregateSchema):
    ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS = 'SeparateByInactiveSeconds'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.separation_interval: timedelta = timedelta(
            seconds=int(self._spec[self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS]))


class ActivityAggregate(StreamingAggregate):
    """ Aggregates activity in blocks separated by periods of inactivity"""

    def evaluate(self) -> None:
        time = self._evaluation_context.global_context['time']

        # If the event time is beyond separation threshold, create a new block
        if self._key and (time < self._start_time - self._schema.separation_interval
                          or time > self._end_time + self._schema.separation_interval):
            self._key = self._prepare_key(self._start_time)
            self.persist(self._start_time)
            self.reset()

        super().evaluate()

        self._key = self._prepare_key(self._start_time)
