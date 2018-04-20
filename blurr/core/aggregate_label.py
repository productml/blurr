from typing import Dict, Any, Type, Union

from blurr.core.aggregate_block import BlockAggregate, BlockAggregateSchema
from blurr.core.base import BaseItemCollection
from blurr.core.errors import SnapshotError
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.field import FieldSchema, Field
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


class LabelAggregateSchema(BlockAggregateSchema):
    """ Schema for Block Aggregation by a Label that can determined by the record being processed """

    ATTRIBUTE_LABEL_FIELDS = 'LabelFields'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)
        self.label_fields: Dict[str, Type[FieldSchema]] = {
            schema_spec[self.ATTRIBUTE_NAME]: self.schema_loader.get_nested_schema_object(
                self.fully_qualified_name, schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec.get(self.ATTRIBUTE_LABEL_FIELDS, [])
        }


class LabelAggregate(BlockAggregate):
    """ Aggregates records in blocks by a label calculated from the record """

    def __init__(self, schema: LabelAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)

        self._label_fields: Dict[str, Field] = {
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.label_fields.items()
        }
        self._key = None

        # Dictionary to store label fields before deciding whether the current state needs to be
        # persisted or not and whether store needs to be queried.
        self._temp_label_fields: Dict[str, Field] = {
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.label_fields.items()
        }
        self._temp_key = None

    def _evaluate_temp_label_fields(self) -> bool:
        if self._needs_evaluation:
            for _, item in self._temp_label_fields.items():
                item.evaluate()
                if item.none_result:
                    return False
        self._temp_key = self._prepare_key(self._temp_label_fields)
        return True

    def _set_label_fields_from_temp(self):
        for name, item in self._temp_label_fields.items():
            self._label_fields[name].restore(item._snapshot)
        self._key = self._prepare_key(self._label_fields)

    def _prepare_key(self, label_fields: Dict[str, Field]):
        """ Generates the Key object based on label """
        return Key(self._identity, self._name + '.' +
                   (':').join([str(item.value) for item in label_fields.values()]))

    def evaluate(self) -> None:
        if not self._evaluate_temp_label_fields():
            return

        if self._key and self._key != self._temp_key:
            # Save the current snapshot with the current timestamp
            self.persist()

            # Try restoring a previous state if it exists, otherwise, reset to create a new state
            snapshot = self._schema.store.get(self._temp_key)
            if snapshot:
                self.restore(snapshot)
            else:
                self.reset()

        if not self._key:
            self._set_label_fields_from_temp()

        super().evaluate()

    def persist(self, timestamp=None) -> None:
        # TODO Refactor keys when refactoring store
        # Timestamp is ignored for now.
        self._schema.store.save(self._prepare_key(self._label_fields), self._snapshot)

    @property
    def _snapshot(self) -> Dict[str, Any]:
        snapshot = super()._snapshot
        try:
            snapshot.update({name: item._snapshot for name, item in self._label_fields.items()})
            return snapshot
        except Exception as e:
            raise SnapshotError('Error while creating snapshot for {}'.format(self._name)) from e

    def restore(self, snapshot: Dict[Union[str, Key], Any]):
        try:
            label_snapshot = snapshot.copy()
            for name, item in self._label_fields.items():
                item.restore(label_snapshot[name])
                del label_snapshot[name]
            super().restore(label_snapshot)
            self._key = self._prepare_key(self._label_fields)
        except Exception as e:
            raise SnapshotError('Error while restoring snapshot: {}'.format(self._snapshot)) from e

    def reset(self):
        super().reset()
        for _, item in self._label_fields.items():
            item.reset()
        self._key = None
