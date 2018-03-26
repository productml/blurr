class InvalidSchemaError(Exception):
    """
    Indicates an error in the schema specification
    """
    pass


class InvalidExpressionError(Exception):
    """
    Indicates that a python expression specified is either non-compilable, or not allowed
    """
    pass


class ExpressionEvaluationError(Exception):
    """
    Error raised during expression evaluation by the interpreter
    """
    pass


class TypeNotFoundError(Exception):
    """
    Indicates dynamic type loading failure if type is not found type map
    """
    pass


class SnapshotError(Exception):
    """
    Indicates issues with serializing the current state of the object
    """
    pass


class StaleSessionError(Exception):
    """
    Indicates that the event being processed cannot be added to the session rollup that is loaded
    """
    pass


class StreamingSourceNotFoundError(Exception):
    """
    Raised when the raw data for streaming is unavailable in the execution context
    """
    pass


class AnchorSessionNotDefinedError(Exception):
    """
    Raised when anchor session is not defined and a WindowTransformer is evaluated.
    """
    pass


class IdentityMismatchError(Exception):
    """
    Raised when the identity of the record being evaluated does not match the
    identity of existing data.
    """
    pass


class PrepareWindowMissingSessionsError(Exception):
    """
    Raised when the window view generated is insufficient as per the window specification.
    """
    pass
