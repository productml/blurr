from abc import ABC
from collections import defaultdict
from enum import Enum
from io import StringIO
from itertools import chain
from os import linesep
from typing import List, Dict, Any, Union, Type, Set


class GenericSchemaError(Exception):
    pass


class BaseSchemaError(Exception, ABC):
    """
    Indicates an error in the schema specification
    """

    def __init__(self, fully_qualified_name: str, spec: Dict[str, Any], attribute: str, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.fully_qualified_name = fully_qualified_name
        self.spec = spec
        self.attribute = attribute

    def __repr__(self):
        return '{cls}: FQN: {fqn}, Attribute: {attribute}'.format(
            cls=self.__class__.__name__, fqn=self.fully_qualified_name, attribute=self.attribute)


class RequiredAttributeError(BaseSchemaError):
    def __str__(self):
        return 'Attribute `{}` must be present under `{}`.'.format(self.attribute,
                                                                   self.fully_qualified_name)


class EmptyAttributeError(BaseSchemaError):
    def __str__(self):
        return 'Attribute `{}` under `{}` cannot be left empty.'.format(
            self.attribute, self.fully_qualified_name)


class InvalidValueError(BaseSchemaError):
    def __init__(self, fully_qualified_name: str, spec: Dict[str, Any], attribute: str,
                 candidates: Set[Any], *args, **kwargs):
        super().__init__(fully_qualified_name, spec, attribute, *args, **kwargs)
        self.candidates = candidates

    def __str__(self):
        return 'Attribute `{attr}` under `{fqn}` must have one of the following values: {candidates}'.format(
            attr=self.attribute,
            fqn=self.fully_qualified_name,
            candidates=' | '.join([str(x) for x in self.candidates]))


class InvalidNumberError(BaseSchemaError):
    def __init__(self, fully_qualified_name: str, spec: Dict[str, Any], attribute: str,
                 value_type: Type, minimum: Any, maximum: Any, *args, **kwargs):
        super().__init__(fully_qualified_name, spec, attribute, *args, **kwargs)
        self.type = value_type
        self.min = minimum
        self.max = maximum

    def __str__(self):
        return 'Attribute `{attr}` under `{fqn}` must be of type `{type}`. {less_than} {greater_than}'.format(
            attr=self.attribute,
            fqn=self.fully_qualified_name,
            type=self.type.__name__,
            greater_than=('Must be greater than ' + str(self.min)) if self.min else '',
            less_than=('Must be lesser than ' + (self.max)) if self.max else '')


class InvalidIdentifierError(BaseSchemaError):
    class Reason(Enum):
        STARTS_WITH_UNDERSCORE = 'Identifiers starting with underscore `_` are reserved'
        STARTS_WITH_RUN = 'Identifiers starting with `run_` are reserved'
        INVALID_PYTHON_IDENTIFIER = 'Identifiers must be valid Python identifiers'

    def __init__(self, fully_qualified_name: str, spec: Dict[str, Any], attribute: str,
                 reason: 'Reason', *args, **kwargs):
        super().__init__(fully_qualified_name, spec, attribute, *args, **kwargs)
        self.reason = reason

    def __str__(self):
        return '`{attribute}: {value}` in section `{name}` is invalid. {reason}.'.format(
            attribute=self.attribute,
            value=self.spec.get(self.attribute, '*missing*'),
            name=self.fully_qualified_name,
            reason=self.reason.value)


class InvalidExpressionError(BaseSchemaError):
    """
    Indicates that a python expression specified is either non-compilable, or not allowed
    """

    def __init__(self, fully_qualified_name: str, spec: Dict[str, Any], attribute: str,
                 error: Exception, *args, **kwargs):
        super().__init__(fully_qualified_name, spec, attribute, *args, **kwargs)
        self.error = error

    def __str__(self):
        return '`{attribute}: {value}` in section `{name}` is invalid Python expression. Compilation error: \n{error}'.format(
            attribute=self.attribute,
            value=self.spec.get(self.attribute, '*missing*'),
            name=self.fully_qualified_name,
            error=str(self.error))


class SchemaErrorCollection:
    def __init__(self, *args):
        self.log: Dict[str, List(BaseSchemaError)] = defaultdict(list)
        for arg in args:
            self.add(arg)

    def add(self, item: Union[BaseSchemaError, List[BaseSchemaError]]):
        if isinstance(item, BaseSchemaError):
            self.log[item.fully_qualified_name].append(item)

        elif isinstance(item, list):
            for i in item:
                self.add(i)

    def merge(self, item: 'SchemaErrorCollection'):
        if not item:
            return

        for k, v in item.log.items():
            self.log[k].extend(v)

    def __str__(self):
        return linesep.join(
            [str(error) for error in self.log.values()]) if len(self.log) > 0 else ''

    def __getitem__(self, item):
        return self.log.get(item, None)

    def __contains__(self, item):
        return self.log.__contains__(item)

    def __iter__(self):
        return iter(self.log.items())

    @property
    def errors(self) -> List[BaseSchemaError]:
        return list(chain.from_iterable(self.log.values()))

    @property
    def has_errors(self) -> bool:
        return len(self.log) > 0

    def raise_errors(self):
        if self.has_errors:
            raise SchemaError(self)


class SchemaErrorCollectionFormatter:
    def __init__(self, **kwargs):
        self.header_separator = kwargs.get('header_separator', '=')
        self.error_separator = kwargs.get('item_separator', '-')
        self.line_separator = kwargs.get('line_separator', linesep)

    def format(self, errors: SchemaErrorCollection) -> Any:
        with StringIO() as result:
            for fqn, errs in errors:
                result.writelines([
                    self.line_separator, fqn, self.line_separator, self.header_separator * len(fqn),
                    self.line_separator
                ])
                for err in errs:
                    result.writelines(['--> ', str(err), self.line_separator])

            return result.getvalue()


class SchemaError(Exception):
    def __init__(self, errors: SchemaErrorCollection, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = errors
        self.formatter = SchemaErrorCollectionFormatter()

    def __str__(self):
        return self.formatter.format(self.errors)

    def __repr__(self):
        return self.__class__.__name__ + linesep + str(self)


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
