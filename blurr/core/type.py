from enum import Enum
from typing import Union, List


class Type(Enum):
    BLURR_TRANSFORM_STREAMING = "blurr:transform:streaming"
    BLURR_TRANSFORM_WINDOW = "blurr:transform:window"
    BLURR_AGGREGATE_BLOCK = "blurr:aggregate:block"
    BLURR_AGGREGATE_LABEL = "blurr:aggregate:label"
    BLURR_AGGREGATE_ACTIVITY = "blurr:aggregate:activity"
    BLURR_AGGREGATE_IDENTITY = "blurr:aggregate:identity"
    BLURR_AGGREGATE_VARIABLE = "blurr:aggregate:variable"
    BLURR_AGGREGATE_WINDOW = "blurr:aggregate:window"
    BLURR_STORE_MEMORY = "blurr:store:memory"
    BLURR_STORE_DYNAMO = "blurr:store:dynamo"
    ANCHOR = "anchor"
    DAY = "day"
    HOUR = "hour"
    COUNT = "count"
    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    FLOAT = "float"
    MAP = "map"
    LIST = "list"
    SET = "set"

    @staticmethod
    def is_type_equal(actual_type: Union[str, 'Type'], expected_type: 'Type') -> bool:
        try:
            return Type(actual_type) == expected_type
        except ValueError:
            return False

    @staticmethod
    def is_type_in(actual_type: Union[str, 'Type'], expected_type: List['Type']) -> bool:
        try:
            return Type(actual_type) in expected_type
        except ValueError:
            return False

    @staticmethod
    def is_store_type(store_type: Union[str, 'Type']) -> bool:
        return Type.is_type_in(store_type, [Type.BLURR_STORE_MEMORY, Type.BLURR_STORE_DYNAMO])


# Override __new__ to handle intialization by value of any case.
Type.__new__ = lambda cls, value: (super(Type, cls).__new__(cls, value.lower())
                                    if isinstance(value, str) else
                                    super(Type, cls).__new__(cls, value))
