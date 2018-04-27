from collections import defaultdict
from enum import Enum
from os import linesep
from typing import List, Dict, Any, Type


class InvalidSchemaError(Exception):
    """
    Indicates an error in the schema specification
    """

    def __init__(self,
                 fully_qualified_name: str,
                 spec: Dict[str, Any],
                 errors: List['InvalidSchemaError'] = None,
                 message: str = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.fully_qualified_name = fully_qualified_name
        self.spec = spec
        self.errors = errors
        self.message = message

    def __str__(self):
        if self.message:
            return self.message
        elif self.errors:
            return linesep.join([str(error) for error in self.errors])
        else:
            return '`{}` is contains invalid schema declarations'.format(self.fully_qualified_name)


class SchemaErrorCollection:
    def __init__(self):
        self.log: Dict[str, List(InvalidSchemaError)] = defaultdict(list)

    def add(self, error: InvalidSchemaError):
        self.log[error.fully_qualified_name].append(error)

    def merge(self, error_log: 'SchemaErrorCollection'):
        for k, v in error_log.log.items():
            self.log[k].extend(v)


class RequiredAttributeError(InvalidSchemaError):
    def __init__(self, fully_qualified_name: str, spec: Dict[str, Any], attribute: str, *args,
                 **kwargs):
        super().__init__(fully_qualified_name, spec, *args, **kwargs)
        self.attribute = attribute
        self.message = 'Attribute `{}` must be present under `{}`.'.format(self.attribute,
                                                                           self.fully_qualified_name)


class EmptyAttributeError(InvalidSchemaError):
    def __init__(self, fully_qualified_name: str, spec: Dict[str, Any], attribute: str, *args,
                 **kwargs):
        super().__init__(fully_qualified_name, spec, *args, **kwargs)
        self.attribute = attribute
        self.message = 'Attribute `{}` under `{}` cannot be left empty.'.format(
            self.attribute, self.fully_qualified_name)


class InvalidIdentifierError(InvalidSchemaError):
    class Reason(Enum):
        STARTS_WITH_UNDERSCORE = 'Identifiers starting with underscore `_` are reserved'
        INVALID_PYTHON_IDENTIFIER = 'Identifiers must be valid Python identifiers'

    def __init__(self, fully_qualified_name: str, spec: Dict[str, Any], attribute: str,
                 reason: 'Reason', *args, **kwargs):
        super().__init__(fully_qualified_name, spec, *args, **kwargs)
        self.attribute = attribute
        self.reason = reason
        self.message = '`{attribute}: {value}` in section `{name}` is invalid. {reason}.'.format(
            attribute=self.attribute,
            value=self.spec[self.attribute],
            name=self.fully_qualified_name,
            reason=reason.value)


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
