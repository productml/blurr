from typing import Dict, Any, List

from blurr.core.aggregate import Aggregate, AggregateSchema
from blurr.core.evaluation import Expression
from blurr.core.schema_loader import SchemaLoader
from blurr.core.type import Type


class BlockAggregateSchema(AggregateSchema):
    """
    Aggregates that handles the block rollup aggregation
    """

    ATTRIBUTE_SPLIT = 'Split'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        # Load type specific attributes
        self.split: Expression = Expression(
            self._spec[self.ATTRIBUTE_SPLIT]) if self.ATTRIBUTE_SPLIT in self._spec else None

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
                'Type': Type.DATETIME,
                'Value': ('time if {aggregate}._start_time is None else time '
                          'if time < {aggregate}._start_time else {aggregate}._start_time'
                          ).format(aggregate=name_in_context)
            },
            {
                'Name': '_end_time',
                'Type': Type.DATETIME,
                'Value': ('time if {aggregate}._end_time is None else time '
                          'if time > {aggregate}._end_time else {aggregate}._end_time'
                          ).format(aggregate=name_in_context)
            },
        ]


class BlockAggregate(Aggregate):
    """
    Manages the aggregates for block based roll-ups of streaming data
    """

    def evaluate(self) -> None:
        """
        Evaluates the current item
        """

        # If a split is imminent, save the current block snapshot with the timestamp
        split_should_be_evaluated = not (self._schema.split is None or self._start_time is None
                                         or self._end_time is None)

        if split_should_be_evaluated and self._schema.split.evaluate(
                self._evaluation_context) is True:
            # Save the current snapshot with the current timestamp
            self.persist(self._start_time)
            # Reset the state of the contents
            self.__init__(self._schema, self._identity, self._evaluation_context)

        super().evaluate()

    def persist(self, timestamp=None) -> None:
        super().persist(timestamp if timestamp else self._start_time)
