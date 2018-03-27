from blurr.core.base import Expression
from blurr.core.errors import IdentityMismatchError
from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.transformer import Transformer, TransformerSchema


class StreamingTransformerSchema(TransformerSchema):
    """
    Represents the schema for processing streaming data.  Handles the streaming specific attributes of the schema
    """

    ATTRIBUTE_IDENTITY = 'Identity'
    ATTRIBUTE_TIME = 'Time'

    def __init__(self, fully_qualified_name: str,
                 schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.identity = Expression(self._spec[self.ATTRIBUTE_IDENTITY])
        self.time = Expression(self._spec[self.ATTRIBUTE_TIME])

    def get_identity(self, context: Context) -> str:
        return self.identity.evaluate(EvaluationContext(context))


class StreamingTransformer(Transformer):
    def __init__(self, schema: TransformerSchema, identity: str,
                 context: Context) -> None:
        super().__init__(schema, identity, context)

    def evaluate_record(self, record: Record):
        # Add source record and time to the global context
        self.evaluation_context.global_add('source', record)
        self.evaluation_context.global_add('time',
                                           self.schema.time.evaluate(
                                               self.evaluation_context))

        record_identity = self.schema.get_identity(
            self.evaluation_context.global_context)
        if self.identity != record_identity:
            raise IdentityMismatchError('Identity in transformer (',
                                        self.identity, ') and new record (',
                                        record_identity, ') do not match')

        self.evaluate()

        # Cleanup source and time form the context
        del self.evaluation_context.global_context['source']
        del self.evaluation_context.global_context['time']
