from blurr.core.aggregate_identity import IdentityAggregateSchema, IdentityAggregate
from blurr.core.aggregate_time import TimeAggregateSchema, TimeAggregate


class BlockAggregateSchema(IdentityAggregateSchema, TimeAggregateSchema):
    """ Rolls up records into aggregate blocks.  Blocks are created when the split condition executes to true.  """

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_DIMENSIONS)


class BlockAggregate(IdentityAggregate, TimeAggregate):
    """
    Manages the aggregates for block based roll-ups of streaming data that are split by time.
    """
    pass
