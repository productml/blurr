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
