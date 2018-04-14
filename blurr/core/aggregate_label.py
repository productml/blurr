from blurr.core.aggregate_block import BlockAggregate, BlockAggregateSchema
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


class LabelAggregateSchema(BlockAggregateSchema):
    """
    Data group that handles the block rollup aggregation
    """

    ATTRIBUTE_LABEL = 'Label'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        # Load type specific attributes
        self.label: Expression = Expression(self._spec[self.ATTRIBUTE_LABEL])


class LabelAggregate(BlockAggregate):
    """
    Manages the aggregates for block based roll-ups of streaming data
    """

    def __init__(self, schema: LabelAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)
        self._label = None

    def evaluate(self) -> None:
        """
        Evaluates the current item
        """

        label = self._schema.label.evaluate(self._evaluation_context)

        if not self._label:
            self._label = label
        elif self._label != label:
            # Save the current snapshot with the current timestamp
            self.persist(self._start_time)
            # Reset the state of the contents
            self.__init__(self._schema, self._identity, self._evaluation_context)
            self._label = label

        super().evaluate()

    def persist(self, timestamp=None) -> None:
        """ Persists the label by combining it with group. """
        # TODO Refactor keys when refactoring store
        self._schema.store.save(Key(self._identity, self._name + '.' + self._label, timestamp),
                                    self._snapshot)

    def finalize(self):
        """ Persist the current frame with time at finalization """
        if self._label:
            self.persist(self._start_time)
