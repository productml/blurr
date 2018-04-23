from blurr.core.aggregate_streaming import StreamingAggregateSchema, StreamingAggregate
from blurr.core.evaluation import EvaluationContext


class IdentityAggregateSchema(StreamingAggregateSchema):
    pass


class IdentityAggregate(StreamingAggregate):
    def __init__(self, schema: StreamingAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)

    def evaluate(self) -> None:
        if not self._needs_evaluation:
            return

        if not self._evaluate_key_fields():
            return

        if self._key and not self._compare_keys_to_fields():
            # Save the current snapshot.
            self.persist()

            # Try restoring a previous state if it exists, otherwise, reset to create a new state
            self._key = self._prepare_key()
            snapshot = self._schema.store.get(self._key)
            if snapshot:
                self.restore(snapshot)
            else:
                self.reset()

        super().evaluate()
        self._key = self._prepare_key()
