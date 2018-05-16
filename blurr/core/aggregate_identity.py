from datetime import datetime
from typing import Dict

from blurr.core.aggregate import AggregateSchema, Aggregate
from blurr.core.evaluation import EvaluationContext
from blurr.core.field import FieldSchema, Field
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


class IdentityAggregateSchema(AggregateSchema):
    """
    Aggregates that handles the block rollup aggregation
    """
    ATTRIBUTE_DIMENSIONS = 'Dimensions'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.dimension_fields: Dict[str, FieldSchema] = {
            schema_spec[self.ATTRIBUTE_NAME]: self.schema_loader.get_nested_schema_object(
                self.fully_qualified_name, schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec.get(self.ATTRIBUTE_DIMENSIONS, [])
        }

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_STORE)


class IdentityAggregate(Aggregate):
    def __init__(self, schema: AggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)

        self._dimension_fields: Dict[str, Field] = {
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.dimension_fields.items()
        }

        # Also add the dimension fields to regular fields so that they get processed by other
        # functions such as snapshot, restore, etc as per normal.
        # We don't add self._dimension_fields here as we want these fields to be separate objects.
        self._fields.update({
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.dimension_fields.items()
        })
        self._existing_key = None

    def run_evaluate(self) -> None:
        if not self._needs_evaluation:
            return

        if not self._evaluate_dimension_fields():
            return

        # First time being run. Load state from store.
        if not self._existing_key:
            self._init_state_from_new_key()

        if self._existing_key and not self._compare_dimensions_to_fields():
            self._persist()
            self._init_state_from_new_key()

        super().run_evaluate()
        self._existing_key = self._prepare_key()

    def _init_state_from_new_key(self):
        snapshot = self._store.get(self._prepare_key())
        if snapshot:
            self.run_restore(snapshot)
        else:
            self.run_reset()

    def _evaluate_dimension_fields(self) -> bool:
        """
        Evaluates the dimension fields. Returns False if any of the fields could not be evaluated.
        """
        for _, item in self._dimension_fields.items():
            item.run_evaluate()
            if item.eval_error:
                return False
        return True

    def _compare_dimensions_to_fields(self) -> bool:
        """ Compares the dimension field values to the value in regular fields."""
        for name, item in self._dimension_fields.items():
            if item.value != self._nested_items[name].value:
                return False
        return True

    def _prepare_key(self, timestamp: datetime = None):
        """ Generates the Key object based on dimension fields. """
        if self._dimension_fields:
            return Key(self._identity, self._name + '.' +
                       (':').join([str(item.value) for item in self._dimension_fields.values()]),
                       timestamp)

        return Key(self._identity, self._name, timestamp)

    def _persist(self, timestamp=None) -> None:
        # TODO Refactor keys when refactoring store
        self._store.save(self._existing_key, self._snapshot)
