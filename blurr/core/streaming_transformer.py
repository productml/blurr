from datetime import datetime

from blurr.core.base import Expression
from blurr.core.errors import IdentityError, TimeError
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

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.identity = Expression(self._spec[self.ATTRIBUTE_IDENTITY])
        self.time = Expression(self._spec[self.ATTRIBUTE_TIME])

    def get_identity(self, context: Context) -> str:
        """
        Evaluates and returns the identity as specified in the schema.
        :param context: Context with the 'source' record set which is used to
        determine the identity.
        :return: The evaluated identity
        :raises: IdentityError if identity cannot be determined.
        """
        identity = self.identity.evaluate(EvaluationContext(None, context))
        if not identity:
            raise IdentityError(
                'Could not determine identity using {}. Evaluation context is {}'.format(
                    self.identity.code_string, context))
        return identity

    def get_time(self, context: Context) -> datetime:
        time = self.time.evaluate(EvaluationContext(None, context))
        if not time or not isinstance(time, datetime):
            raise TimeError('Could not determine time using {}. Evaluation context is {}'.format(
                self.time.code_string, context))
        return time


class StreamingTransformer(Transformer):
    def __init__(self, schema: TransformerSchema, identity: str, context: Context) -> None:
        super().__init__(schema, identity, context)
        self._evaluation_context.global_add('identity', self._identity)

    def evaluate_record(self, record: Record):
        """
        Evaluates and updates data in the StreamingTransformer.
        :param record: The 'source' record used for the update.
        :raises: IdentityError if identity is different from the one used during
        initialization.
        """
        # Add source record and time to the global context
        self._evaluation_context.global_add('source', record)
        self._evaluation_context.global_add('time',
                                            self._schema.time.evaluate(self._evaluation_context))

        record_identity = self._schema.get_identity(self._evaluation_context.global_context)
        if self._identity != record_identity:
            raise IdentityError(
                'Identity in transformer ({}) and new record ({}) do not match'.format(
                    self._identity, record_identity))

        self.evaluate()

        # Cleanup source and time form the context
        del self._evaluation_context.global_context['source']
        del self._evaluation_context.global_context['time']
