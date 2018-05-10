from collections import defaultdict
from datetime import datetime
from typing import Dict

from blurr.core.aggregate_block import BlockAggregate
from blurr.core.base import BaseSchema
from blurr.core.evaluation import EvaluationContext
from blurr.core.schema_loader import SchemaLoader


class AnchorSchema(BaseSchema):
    """
    Represents the schema for the Anchor specified in a window DTC.
    """

    ATTRIBUTE_CONDITION = 'Condition'
    ATTRIBUTE_MAX = 'Max'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.condition = self.build_expression(self.ATTRIBUTE_CONDITION)
        self.max = self._spec[self.ATTRIBUTE_MAX] if self.ATTRIBUTE_MAX in self._spec else None

    def validate_schema_spec(self) -> None:
        self.validate_required_attributes(self.ATTRIBUTE_CONDITION, self.ATTRIBUTE_MAX)


class Anchor:
    def __init__(self, schema: AnchorSchema):
        self._schema = schema
        self._condition_met: Dict[datetime, int] = defaultdict(int)
        self.anchor_block = None

    def evaluate_anchor(self, block: BlockAggregate, evaluation_context: EvaluationContext) -> bool:
        self.anchor_block = block
        if self.max_condition_met(block):
            return False

        if self._schema.condition.evaluate(evaluation_context):
            return True

        return False

    def add_condition_met(self):
        self._condition_met[self.anchor_block._start_time.date()] += 1

    def max_condition_met(self, block: BlockAggregate) -> bool:
        if self._schema.max is None:
            return False
        return self._condition_met[block._start_time.date()] >= self._schema.max
