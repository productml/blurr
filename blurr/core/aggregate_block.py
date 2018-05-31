from abc import ABC
from typing import Dict, Any, List

from blurr.core.aggregate import Aggregate, AggregateSchema
from blurr.core.aggregate_identity import IdentityAggregateSchema, IdentityAggregate
from blurr.core.aggregate_time import TimeAggregateSchema, TimeAggregate
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key, KeyType
from blurr.core.type import Type
from blurr.core.validator import ATTRIBUTE_INTERNAL


class BlockAggregateSchema(IdentityAggregateSchema, TimeAggregateSchema):
    """ Rolls up records into aggregate blocks.  Blocks are created when the split condition executes to true.  """

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_DIMENSIONS)

    def extend_schema_spec(self) -> None:
        """ Injects the block start and end times """
        super().extend_schema_spec()


class BlockAggregate(IdentityAggregate, TimeAggregate):
    """
    Manages the aggregates for block based roll-ups of streaming data that are split by time.
    """
    pass