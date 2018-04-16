from typing import Dict, Any

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

    def extend_schema(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """ Injects the label field """

        label_field = {'Name': '_label', 'Type': 'string', 'Value': spec[self.ATTRIBUTE_NAME] + '._label_value'}
        spec[self.ATTRIBUTE_FIELDS].insert(0, label_field)

        self.schema_loader.add_schema(label_field, self.fully_qualified_name)

        return super().extend_schema(spec)


class LabelAggregate(BlockAggregate):
    """
    Manages the aggregates for block based roll-ups of streaming data
    """

    def __init__(self, schema: LabelAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)
        self._label_value = None

    def evaluate(self) -> None:
        """
        Evaluates the current item
        """

        label = str(self._schema.label.evaluate(self._evaluation_context))

        if self._label_value and self._label_value != label:
            # Save the current snapshot with the current timestamp
            self.persist()
            # Reset the state of the contents
            self.reset()

        self._label_value = label

        super().evaluate()

    def persist(self, timestamp=None) -> None:
        """ Persists the label by combining it with group. """
        # TODO Refactor keys when refactoring store
        self._schema.store.save(Key(self._identity, self._name + '.' + self._label_value,
                                    timestamp if timestamp else self._start_time), self._snapshot)
