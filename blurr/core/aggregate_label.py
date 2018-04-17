from typing import Dict, Any

from blurr.core.aggregate_block import BlockAggregate, BlockAggregateSchema
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


class LabelAggregateSchema(BlockAggregateSchema):
    """ Schema for Block Aggregation by a Label that can determined by the record being processed """

    ATTRIBUTE_LABEL = 'Label'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)
        self.label: Expression = Expression(self._spec[self.ATTRIBUTE_LABEL])

    def extend_schema(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """ Introduces `label` as a field in the aggregate """
        label_field = {
            'Name': '_label',
            'Type': 'string',
            'Value': spec[self.ATTRIBUTE_NAME] + '._label_value'
        }
        spec[self.ATTRIBUTE_FIELDS].insert(0, label_field)

        self.schema_loader.add_schema(label_field, self.fully_qualified_name)

        return super().extend_schema(spec)


class LabelAggregate(BlockAggregate):
    """ Aggregates records in blocks by a label calculated from the record """

    def __init__(self, schema: LabelAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)
        self._label_value = None

    def evaluate(self) -> None:

        label = str(self._schema.label.evaluate(self._evaluation_context))

        if self._label_value and self._label_value != label:
            # Save the current snapshot with the current timestamp
            self.persist()

            # Try restoring a previous state if it exists, otherwise, reset to create a new state
            snapshot = self._schema.store.get(self.prepare_key(label))
            if snapshot:
                self.restore(snapshot)
            else:
                self.reset()

        self._label_value = label

        super().evaluate()

    def prepare_key(self, label: str):
        """ Generates the Key object based on label """
        return Key(self._identity, self._name + '.' + label)

    def persist(self, timestamp=None) -> None:
        # TODO Refactor keys when refactoring store
        # Timestamp is ignored for now.
        self._schema.store.save(self.prepare_key(self._label_value), self._snapshot)
