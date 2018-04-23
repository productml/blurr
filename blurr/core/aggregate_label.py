from typing import Dict, Any

from blurr.core.aggregate_streaming import StreamingAggregate, StreamingAggregateSchema
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.field import FieldSchema, Field
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


class LabelAggregateSchema(StreamingAggregateSchema):
    """ Schema for Block Aggregation by a Label that can determined by the record being processed """

    ATTRIBUTE_LABEL_FIELDS = 'LabelFields'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)
        self.label_fields: Dict[str, Type[FieldSchema]] = {
            schema_spec[self.ATTRIBUTE_NAME]: self.schema_loader.get_nested_schema_object(
                self.fully_qualified_name, schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec.get(self.ATTRIBUTE_LABEL_FIELDS, [])
        }


class LabelAggregate(StreamingAggregate):
    """ Aggregates records in blocks by a label calculated from the record """

    def __init__(self, schema: LabelAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)

        self._label_fields: Dict[str, Field] = {
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.label_fields.items()
        }

        # Also add the label fields to regular fields so that they get processed by other functions
        # such as snapshot, restore, etc as per normal.
        # We don't add self._label_fields here as we want these fields to be separate objects.
        self._fields.update({
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.label_fields.items()
        })
        self._key = None

    def _evaluate_label_fields(self) -> bool:
        """
        Evaluates the label fields. Returns False if any of the fields could not be evaluated.
        """
        if self._needs_evaluation:
            for _, item in self._label_fields.items():
                item.evaluate()
                if item.eval_error:
                    return False
        return True

    def _compare_label_fields_to_fields(self) -> bool:
        """ Compares the label field values to the same fields in regular fields"""
        for name, item in self._label_fields.items():
            if item.value != self._nested_items[name].value:
                return False
        return True

    def _prepare_key(self):
        """ Generates the Key object based on label """
        return Key(self._identity, self._name + '.' +
                   (':').join([str(item.value) for item in self._label_fields.values()]))

    def evaluate(self) -> None:
        if not self._evaluate_label_fields():
            return

        if self._key and not self._compare_label_fields_to_fields():
            # Save the current snapshot.
            self.persist()

            # Try restoring a previous state if it exists, otherwise, reset to create a new state
            self._key = self._prepare_key()
            snapshot = self._schema.store.get(self._key)
            if snapshot:
                self.restore(snapshot)
            else:
                self.reset()

        if not self._key:
            self._key = self._prepare_key()

        super().evaluate()

    def persist(self, timestamp=None) -> None:
        # TODO Refactor keys when refactoring store
        # Timestamp is ignored for now.
        self._schema.store.save(self._key, self._snapshot)

    def reset(self):
        super().reset()
        self._key = None
