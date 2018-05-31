from abc import ABC
from typing import List, Dict, Any

from blurr.core.aggregate import AggregateSchema, Aggregate
from blurr.core.type import Type
from blurr.core.validator import ATTRIBUTE_INTERNAL


class TimeAggregateSchema(AggregateSchema, ABC):
    """ Rolls up records into aggregate blocks.  Blocks are created when the split condition executes to true.  """

    def extend_schema_spec(self) -> None:
        """ Injects the block start and end times """
        super().extend_schema_spec()

        if self.ATTRIBUTE_FIELDS in self._spec:
            # Add new fields to the schema spec. Since `_identity` is added by the super, new elements are added after
            predefined_field = self._build_time_fields_spec(self._spec[self.ATTRIBUTE_NAME])
            self._spec[self.ATTRIBUTE_FIELDS][1:1] = predefined_field

            # Add new field schema to the schema loader
            for field_schema in predefined_field:
                self.schema_loader.add_schema_spec(field_schema, self.fully_qualified_name)

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
                'Type': Type.DATETIME,
                'Value': ('time if {aggregate}._start_time is None else time '
                          'if time < {aggregate}._start_time else {aggregate}._start_time'
                          ).format(aggregate=name_in_context),
                ATTRIBUTE_INTERNAL: True
            },
            {
                'Name': '_end_time',
                'Type': Type.DATETIME,
                'Value': ('time if {aggregate}._end_time is None else time '
                          'if time > {aggregate}._end_time else {aggregate}._end_time'
                          ).format(aggregate=name_in_context),
                ATTRIBUTE_INTERNAL: True
            },
        ]


class TimeAggregate(Aggregate, ABC):
    pass
