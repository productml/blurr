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


class StaleBlockError(Exception):
    """
    Indicates that the event being processed cannot be added to the block rollup that is loaded
    """
    pass


class StreamingSourceNotFoundError(Exception):
    """
    Raised when the raw data for streaming is unavailable in the execution context
    """
    pass


class AnchorBlockNotDefinedError(Exception):
    """
    Raised when anchor block is not defined and a WindowTransformer is evaluated.
    """
    pass


class IdentityError(Exception):
    """
    Raised when there is an error in the identity determination of a record.
    """
    pass


class TimeError(Exception):
    """
    Raised when there is an error in determining the time of the record.
    """
    pass


class PrepareWindowMissingBlocksError(Exception):
    """
    Raised when the window view generated is insufficient as per the window specification.
    """
    pass


class MissingAttributeError(Exception):
    """
    Raised when the name of the item being retrieved does not exist in the nested items.
    """
    pass
