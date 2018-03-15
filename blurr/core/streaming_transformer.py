from typing import Any, Dict

from blurr.core.base import Expression
from blurr.core.errors import StreamingSourceNotFoundError
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.record import Record
from blurr.core.transformer import Transformer, TransformerSchema
from blurr.core.store import Store
from blurr.core.session_data_group import SessionDataGroup


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

    def get_identity(self, context: Context) -> str:
        return self.identity.evaluate(EvaluationContext(context))


class StreamingTransformer(Transformer):
    def __init__(self, store: Store, schema: TransformerSchema, identity: str,
                 context: Context) -> None:
        super().__init__(store, schema, identity, context)
        self.evaluation_context.global_add('identity', self.identity)

    def evaluate_record(self, record: Record):
        # Add source record and time to the global context
        self.evaluation_context.global_add('source', record)
        self.evaluation_context.global_add('time',
                                           self.schema.time.evaluate(
                                               self.evaluation_context))

        self.evaluate()

        # Cleanup source and time form the context
        del self.evaluation_context.global_context['source']
        del self.evaluation_context.global_context['time']

    def evaluate(self) -> None:
        """
        Evaluates the current item
        :returns An evaluation result object containing the result, or reasons why
        evaluation failed
        """

        if 'source' not in self.evaluation_context.global_context:
            raise StreamingSourceNotFoundError()

        if not self.needs_evaluation:
            return

        for _, item in self.nested_items.items():
            if isinstance(item, SessionDataGroup) and item.split_now:
#                self.store.save(self.identity, item.name)
                item.reset()

        super().evaluate()
