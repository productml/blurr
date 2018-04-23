from datetime import datetime
from abc import ABC
from typing import Dict, Any, List, Type

from blurr.core.aggregate import Aggregate, AggregateSchema
from blurr.core.evaluation import EvaluationContext
from blurr.core.field import FieldSchema, Field
from blurr.core.loader import TypeLoader
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key


class StreamingAggregateSchema(AggregateSchema, ABC):
    """
    Aggregates that handles the block rollup aggregation
    """
    KEY_FIELDS = 'Keys'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.key_fields: Dict[str, Type[FieldSchema]] = {
            schema_spec[self.ATTRIBUTE_NAME]: self.schema_loader.get_nested_schema_object(
                self.fully_qualified_name, schema_spec[self.ATTRIBUTE_NAME])
            for schema_spec in self._spec.get(self.KEY_FIELDS, [])
        }

    def extend_schema(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """ Injects the block start and end times """

        # Add new fields to the schema spec
        predefined_field = self._build_time_fields_spec(spec[self.ATTRIBUTE_NAME])
        spec[self.ATTRIBUTE_FIELDS][0:0] = predefined_field

        # Add new field schema to the schema loader
        for field_schema in predefined_field:
            self.schema_loader.add_schema(field_schema, self.fully_qualified_name)

        return super().extend_schema(spec)

    @staticmethod
    def _build_time_fields_spec(name_in_context: str) -> List[Dict[str, Any]]:
        """
        Constructs the spec for predefined fields that are to be included in the master spec prior to schema load
        :param name_in_context: Name of the current object in the context
        :return:
        """
        return [
            {
                'Name': '_start_time',
                'Type': 'datetime',
                'Value': ('time if {aggregate}._start_time is None else time '
                          'if time < {aggregate}._start_time else {aggregate}._start_time'
                          ).format(aggregate=name_in_context)
            },
            {
                'Name': '_end_time',
                'Type': 'datetime',
                'Value': ('time if {aggregate}._end_time is None else time '
                          'if time > {aggregate}._end_time else {aggregate}._end_time'
                          ).format(aggregate=name_in_context)
            },
        ]


class StreamingAggregate(Aggregate, ABC):
    def __init__(self, schema: StreamingAggregateSchema, identity: str,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, identity, evaluation_context)

        self._key_fields: Dict[str, Field] = {
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.key_fields.items()
        }

        # Also add the label fields to regular fields so that they get processed by other functions
        # such as snapshot, restore, etc as per normal.
        # We don't add self._label_fields here as we want these fields to be separate objects.
        self._fields.update({
            name: TypeLoader.load_item(item_schema.type)(item_schema, self._evaluation_context)
            for name, item_schema in self._schema.key_fields.items()
        })
        self._key = None

    def _evaluate_key_fields(self) -> bool:
        """
        Evaluates the label fields. Returns False if any of the fields could not be evaluated.
        """
        for _, item in self._key_fields.items():
            item.evaluate()
            if item.eval_error:
                return False
        return True

    def _compare_keys_to_fields(self) -> bool:
        """ Compares the key field values to the value in regular fields."""
        for name, item in self._key_fields.items():
            if item.value != self._nested_items[name].value:
                return False
        return True

    def _prepare_key(self, timestamp: datetime = None):
        """ Generates the Key object based on key fields. """
        if self._key_fields:
            return Key(self._identity, self._name + '.' +
                       (':').join([str(item.value) for item in self._key_fields.values()]),
                       timestamp)

        return Key(self._identity, self._name, timestamp)

    def persist(self, timestamp=None) -> None:
        # TODO Refactor keys when refactoring store
        self._schema.store.save(self._key, self._snapshot)
