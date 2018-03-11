class InvalidSchemaException(Exception):
    pass


class InvalidExpressionException(Exception):
    pass


class ExpressionEvaluationException(Exception):
    pass


class TypeNotFoundException(Exception):
    pass


class SnapshotException(Exception):
    pass


class StaleSessionException(Exception):
    pass
