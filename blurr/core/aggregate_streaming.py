from abc import ABC
from typing import Dict, Any, List

from blurr.core.aggregate import Aggregate, AggregateSchema
from blurr.core.schema_loader import SchemaLoader


class StreamingAggregateSchema(AggregateSchema, ABC):
    """
    Aggregates that handles the block rollup aggregation
    """

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

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
    """
    Manages the aggregates for block based roll-ups of streaming data
    """
    pass
