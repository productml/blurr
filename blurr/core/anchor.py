from datetime import datetime
from typing import Dict, Any

from blurr.core.base import BaseSchema, BaseItem
from blurr.core.evaluation import Expression, EvaluationContext


class AnchorSchema(BaseSchema):
    ATTRIBUTE_CONDITION = 'Condition'
    ATTRIBUTE_MAX = 'Max'

    def __init__(self, spec: Dict[str, Any]) -> None:
        # Inject name and type as expected by BaseSchema
        spec[self.ATTRIBUTE_NAME] = 'anchor'
        spec[self.ATTRIBUTE_TYPE] = 'anchor'
        super().__init__(spec)

    def load(self, spec: Dict[str, Any]) -> None:
        self.condition = Expression(spec[self.ATTRIBUTE_CONDITION])
        self.max = spec[
            self.ATTRIBUTE_MAX] if self.ATTRIBUTE_MAX in spec else None

    def validate(self, spec: Dict[str, Any]) -> None:
        self.validate_required_attribute(spec, self.ATTRIBUTE_CONDITION)


class Anchor(BaseItem):
    def __init__(self, schema: AnchorSchema,
                 evaluation_context: EvaluationContext):
        super().__init__(schema, evaluation_context)
        self.condition_met: Dict[datetime, int] = {}
        self.anchor_session = None

    def evaluate_anchor(self, session) -> bool:
        self.anchor_session = session
        if self.max_condition_met(session):
            return False

        if self.evaluate():
            self.condition_met[self.anchor_session.start_time.date(
            )] = self.condition_met.get(self.anchor_session.start_time.date(),
                                        0) + 1
            return True

        return False

    def evaluate(self) -> bool:
        return self.schema.condition.evaluate(self.evaluation_context)

    def max_condition_met(self, session) -> bool:
        if self.condition_met.get(session.start_time.date(),
                                  0) >= self.schema.max:
            return True

        return False

    def restore(self, snapshot) -> None:
        pass

    @property
    def snapshot(self):
        pass
