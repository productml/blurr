from typing import Dict, Any, List

from blurr.core.data_group import DataGroup, DataGroupSchema
from blurr.core.evaluation import Expression
from blurr.core.schema_loader import SchemaLoader


class BlockDataGroupSchema(DataGroupSchema):
    """
    Data group that handles the block rollup aggregation
    """

    ATTRIBUTE_SPLIT = 'Split'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        # Load type specific attributes
        self.split: Expression = Expression(
            self._spec[self.ATTRIBUTE_SPLIT]
        ) if self.ATTRIBUTE_SPLIT in self._spec else None

    def extend_schema(self):
        # Alter the spec to introduce the block start and end time implicitly
        # handled fields
        predefined_field = self._build_predefined_fields_spec(
            self._spec[self.ATTRIBUTE_NAME])
        self._spec[self.ATTRIBUTE_FIELDS][0:0] = predefined_field
        for field_schema in predefined_field:
            self.schema_loader.add_schema(field_schema,
                                          self.fully_qualified_name)

    @staticmethod
    def _build_predefined_fields_spec(
            name_in_context: str) -> List[Dict[str, Any]]:
        """
        Constructs the spec for predefined fields that are to be included in the master spec prior to schema load
        :param name_in_context: Name of the current object in the context
        :return:
        """
        return [
            {
                'Name': 'start_time',
                'Type': 'datetime',
                'Value': (
                    'time if {data_group}.start_time is None else time '
                    'if time < {data_group}.start_time else {data_group}.start_time'
                ).format(data_group=name_in_context)
            },
            {
                'Name': 'end_time',
                'Type': 'datetime',
                'Value': (
                    'time if {data_group}.end_time is None else time '
                    'if time > {data_group}.end_time else {data_group}.end_time'
                ).format(data_group=name_in_context)
            },
        ]


class BlockDataGroup(DataGroup):
    """
    Manages the aggregates for block based roll-ups of streaming data
    """

    def evaluate(self) -> None:
        """
        Evaluates the current item
        """

        # If a split is imminent, save the current block snapshot with the timestamp
        split_should_be_evaluated = not (self.schema.split is None
                                         or self.start_time is None
                                         or self.end_time is None)

        if split_should_be_evaluated and self.schema.split.evaluate(
                self.evaluation_context) is True:
            # Save the current snapshot with the current timestamp
            self.persist(self.start_time)
            # Reset the state of the contents
            self.__init__(self.schema, self.identity, self.evaluation_context)

        super().evaluate()
