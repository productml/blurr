from datetime import datetime

from blurr.core.errors import IdentityError, TimeError
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.transformer import Transformer, TransformerSchema
from blurr.core.evaluation import ScopedEvaluationContext, EvaluationContext, Context


class StreamingTransformerSchema(TransformerSchema):
    """
    Represents the schema for processing streaming data.  Handles the streaming specific attributes of the schema
    """

    ATTRIBUTE_IDENTITY = 'Identity'
    ATTRIBUTE_TIME = 'Time'

    def __init__(self, fully_qualified_name: str, schema_loader: SchemaLoader) -> None:
        super().__init__(fully_qualified_name, schema_loader)

        self.identity = self.build_expression(self.ATTRIBUTE_IDENTITY)
        self.time = self.build_expression(self.ATTRIBUTE_TIME)

    def validate_schema_spec(self) -> None:
        super().validate_schema_spec()
        self.validate_required_attributes(self.ATTRIBUTE_IDENTITY, self.ATTRIBUTE_TIME,
                                          self.ATTRIBUTE_STORES)

    def get_identity(self, record: Record, evaluation_context: EvaluationContext = None) -> str:
        """
        Evaluates and returns the identity as specified in the schema.
        :param record: Record which is used to determine the identity. Setting none assumes record is in context.
        :return: The evaluated identity
        :raises: IdentityError if identity cannot be determined.
        """
        identity = None
        context = evaluation_context if evaluation_context else self.schema_context.context

        if record is not None:
            context.add_record(record)
            identity = self.identity.evaluate(context)
            context.remove_record()
        else:
            identity = self.identity.evaluate(context)

        if not identity:
            raise IdentityError('Could not determine identity using {}. Record is {}'.format(
                self.identity.code_string, record))

        return identity

    def get_time(self, record: Record, evaluation_context: EvaluationContext = None) -> datetime:
        context = evaluation_context if evaluation_context else self.schema_context.context
        time = None
        if record is not None:
            context.add_record(record)
            time = self.time.evaluate(context)
            context.remove_record()
        else:
            time = self.time.evaluate(context)

        if not time or not isinstance(time, datetime):
            raise TimeError('Could not determine time using {}.  Record is {}'.format(
                self.time.code_string, record))

        return time


class StreamingTransformer(Transformer):
    def __init__(self, schema: TransformerSchema, identity: str) -> None:
        super().__init__(schema, identity)
        self._evaluation_context.global_add('identity', self._identity)

    def run_evaluate(self, record: Record):
        """
        Evaluates and updates data in the StreamingTransformer.
        :param record: The 'source' record used for the update.
        :raises: IdentityError if identity is different from the one used during
        initialization.
        """
        with ScopedEvaluationContext(self._evaluation_context, Context({
                'source': record
        })) as evaluation_context:
            record_identity = self._schema.get_identity(None, evaluation_context)
            if self._identity != record_identity:
                raise IdentityError(
                    'Identity in transformer ({}) and new record ({}) do not match'.format(
                        self._identity, record_identity))

            # Add source record and time to the global context

            evaluation_context.global_add('time', self._schema.time.evaluate(evaluation_context))
            super().run_evaluate(evaluation_context=evaluation_context)

            # Cleanup source and time form the context
            evaluation_context.global_remove('time')
