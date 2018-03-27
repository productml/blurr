from datetime import datetime
from typing import Dict, Any

from collections import defaultdict

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import Expression, EvaluationContext
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroup


class AnchorSchema(BaseSchema):
    """
    Represents the schema for the Anchor specified in a window DTC.
    """
    ATTRIBUTE_CONDITION = 'Condition'
    ATTRIBUTE_MAX = 'Max'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.condition = Expression(self._spec[self.ATTRIBUTE_CONDITION])
        self.max = self._spec[
            self.ATTRIBUTE_MAX] if self.ATTRIBUTE_MAX in self._spec else None


class Anchor(BaseItem):
    def __init__(self, schema: AnchorSchema,
                 evaluation_context: EvaluationContext):
        super().__init__(schema, evaluation_context)
        self.condition_met: Dict[datetime, int] = defaultdict(int)
        self.anchor_session = None

    def evaluate_anchor(self, session: SessionDataGroup) -> bool:
        self.anchor_session = session
        if self.max_condition_met(session):
            return False

        if self.evaluate():
            self.condition_met[self.anchor_session.start_time.date()] += 1
            return True

        return False

    def evaluate(self) -> bool:
        return self.schema.condition.evaluate(self.evaluation_context)

    def max_condition_met(self, session: SessionDataGroup) -> bool:
        return self.condition_met[session.start_time.date()] >= self.schema.max

    def restore(self, snapshot: Dict[str, Any]) -> BaseItem:
        pass

    @property
    def snapshot(self):
        pass
