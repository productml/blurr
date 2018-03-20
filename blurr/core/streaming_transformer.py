from typing import Any, Dict

from blurr.core.base import Expression
from blurr.core.errors import StreamingSourceNotFoundError
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.record import Record
from blurr.core.transformer import Transformer, TransformerSchema
from blurr.core.store import Store, Key
from blurr.core.session_data_group import SessionDataGroup


class StreamingTransformerSchema(TransformerSchema):
    """
    Represents the schema for processing streaming data.  Handles the streaming specific attributes of the schema
    """

    ATTRIBUTE_IDENTITY = 'Identity'
    ATTRIBUTE_TIME = 'Time'

    def load(self) -> None:
        # Ensure that the base loader is invoked
        super().load()

        # Load the schema specific attributes
        self.identity = Expression(self._spec[self.ATTRIBUTE_IDENTITY])
        self.time = Expression(self._spec[self.ATTRIBUTE_TIME])

    def get_identity(self, context: Context) -> str:
        return self.identity.evaluate(EvaluationContext(context))


class StreamingTransformer(Transformer):
    def __init__(self, schema: TransformerSchema, identity: str,
                 context: Context) -> None:
        super().__init__(schema, identity, context)
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
