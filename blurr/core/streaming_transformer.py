from typing import Any, Dict

from blurr.core.base import Expression
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.transformer import Transformer, TransformerSchema


class StreamingTransformerSchema(TransformerSchema):
    """
    Represents the schema for processing streaming data.  Handles the streaming specific attributes of the schema
    """

    ATTRIBUTE_IDENTITY = 'Identity'
    ATTRIBUTE_TIME = 'Time'

    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)

    def validate(self, spec: Dict[str, Any]) -> None:
        # Ensure that the base validator is invoked
        super().validate(spec)

        # Validate schema specific attributes
        self.validate_required_attribute(spec, self.ATTRIBUTE_IDENTITY)
        self.validate_required_attribute(spec, self.ATTRIBUTE_TIME)

    def load(self, spec: Dict[str, Any]) -> None:
        # Ensure that the base loader is invoked
        super().load(spec)

        # Load the schema specific attributes
        self.identity = Expression(spec[self.ATTRIBUTE_IDENTITY])
        self.time = Expression(spec[self.ATTRIBUTE_TIME])

    def get_identity(self, source_context: EvaluationContext) -> str:
        return self.identity.evaluate(source_context)


class StreamingTransformer(Transformer):
    def __init__(self, schema: TransformerSchema, identity,
                 evaluation_context: EvaluationContext) -> None:
        super().__init__(schema, evaluation_context)
        self._identity = identity
        self.evaluation_context.global_add('identity', self._identity)

    def set_source_context(self, source_context: EvaluationContext) -> None:
        self.evaluation_context.global_context.merge(
            source_context.global_context)
        self.evaluation_context.local_include(source_context.local_context)
        self.evaluation_context.global_add('time',
                                           self.schema.time.evaluate(
                                               self.evaluation_context))
