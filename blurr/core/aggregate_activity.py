from datetime import timedelta, datetime, timezone

from blurr.core.aggregate_time import TimeAggregateSchema, TimeAggregate
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType


class ActivityAggregateSchema(TimeAggregateSchema):
    """ Aggregates activity into blocks by partitioning around periods of inactivity """

    ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS = 'SeparateByInactiveSeconds'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        inactive_seconds = self._spec.get(self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS, 0)
        self.separation_interval: timedelta = timedelta(
            seconds=int(inactive_seconds) if str(inactive_seconds).isdigit() else 0)
        self.key_type = KeyType.TIMESTAMP

    def validate_schema_spec(self) -> None:
        super(TimeAggregateSchema, self).validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS)
        self.validate_number_attribute(self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS, int, 1)


class ActivityAggregate(TimeAggregate):
    """ Aggregates activity in blocks separated by periods of inactivity"""

    def run_evaluate(self) -> None:
        time = self._evaluation_context.global_context['time']

        if not self._start_time:
            self._load_old_state()

        # If the event time is beyond separation threshold, create a new block
        if self._start_time and (time < self._start_time - self._schema.separation_interval
                                 or time > self._end_time + self._schema.separation_interval):
            self._persist()
            self.run_reset()

        super().run_evaluate()

    def _load_old_state(self):
        most_recent_block = self._store.get_range(
            Key(self._schema.key_type, self._identity, self._name), datetime.now(timezone.utc),
            None, -1)
        if most_recent_block:
            self.run_restore(most_recent_block[0][1])

    @property
    def _key(self):
        return Key(self._schema.key_type, self._identity, self._name, [], self._start_time)
