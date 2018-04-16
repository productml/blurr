from datetime import timedelta

from blurr.core.aggregate_block import BlockAggregate, BlockAggregateSchema
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


class ActivityAggregateSchema(BlockAggregateSchema):
    ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS = 'SeparateByInactiveSeconds'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        # Load type specific attributes
        self.separation_interval: timedelta = timedelta(
            seconds=int(self._spec[self.ATTRIBUTE_SEPARATE_BY_INACTIVE_SECONDS]))


class ActivityAggregate(BlockAggregate):
    """ Aggregates activity in blocks separated by periods of inactivity"""

    def __init__(self, schema: ActivityAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)
        self._label = None

    def evaluate(self) -> None:
        time = self._evaluation_context.global_context['time']
        if time < self._start_time - self._schema.separation_interval or \
                time > self._end_time + self._schema.separation_interval:
            # Save the current snapshot with the current timestamp
            self.persist(self._start_time)
            # Reset the state of the contents
            self.__init__(self._schema, self._identity, self._evaluation_context)

        super().evaluate()

    def finalize(self):
        """ Persist the current frame with time at finalization """
        if self._start_time:
            self.persist(self._start_time)
