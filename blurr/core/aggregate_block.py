from typing import Dict, Any, List

from blurr.core.aggregate import Aggregate, AggregateSchema
from blurr.core.evaluation import Expression
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type
from blurr.core.validator import ATTRIBUTE_INTERNAL


class BlockAggregateSchema(AggregateSchema):
    """ Rolls up records into aggregate blocks.  Blocks are created when the split condition executes to true.  """

    ATTRIBUTE_SPLIT = 'Split'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        # Load type specific attributes
        self.split: Expression = self.build_expression(self.ATTRIBUTE_SPLIT)

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_STORE, self.ATTRIBUTE_SPLIT)

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


class BlockAggregate(Aggregate):
    """
    Manages the aggregates for block based roll-ups of streaming data
    """

    def run_evaluate(self) -> None:
        """
        Evaluates the current item
        """

        # If a split is imminent, save the current block snapshot with the timestamp
        split_should_be_evaluated = not (self._schema.split is None or self._start_time is None
                                         or self._end_time is None)

        if split_should_be_evaluated and self._schema.split.evaluate(
                self._evaluation_context) is True:
            # Save the current snapshot with the current timestamp
            self._persist(self._start_time)
            # Reset the state of the contents
            self.__init__(self._schema, self._identity, self._evaluation_context)

        super().run_evaluate()

    def _persist(self, timestamp=None) -> None:
        super()._persist(timestamp if timestamp else self._start_time)
