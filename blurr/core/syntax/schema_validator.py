import ast

import re
from yamale import yamale
from yamale.schema import Data
from yamale.validators import DefaultValidators, Validator

from blurr.core.errors import InvalidSchemaError


def is_expression(s) -> bool:
    try:
        ast.parse(s)
    except SyntaxError:
        return False
    return True


def is_identifier(s) -> bool:
    return len(re.findall(r"[^\S]", s)) == 0


class DataType(Validator):
    TAG = 'data_type'

    VALUES = [
        'integer', 'boolean', 'string', 'datetime', 'float', 'map', 'list',
        'set'
    ]

    def _is_valid(self, value):
        return value in self.VALUES

    def get_name(self):
        return "DTC Valid Data Type"


class Identifier(Validator):
    TAG = 'identifier'

    def _is_valid(self, value):
        return is_identifier(value)

    def get_name(self):
        return "Identifier"


class Expression(Validator):
    TAG = 'expression'

    def _is_valid(self, value):
        return is_expression(value)

    def get_name(self):
        return "Expression"


VALIDATORS = {
    **DefaultValidators.copy(), "data_type": DataType,
    "identifier": Identifier,
    "expression": Expression
}

STREAMING_SCHEMA = yamale.make_schema(
    'blurr/core/syntax/dtc_streaming_schema.yml', validators=VALIDATORS)

WINDOW_SCHEMA = yamale.make_schema(
    'blurr/core/syntax/dtc_window_schema.yml', validators=VALIDATORS)


def _validate_window(dtc_dict, name):
    try:
        WINDOW_SCHEMA.validate(Data(dtc_dict, name))
    except ValueError as e:
        raise InvalidSchemaError(str(e))


def _validate_streaming(dtc_dict, name):
    try:
        STREAMING_SCHEMA.validate(Data(dtc_dict, name))
    except ValueError as e:
        raise InvalidSchemaError(str(e))


def validate(dtc_dict, name="dtc"):
    if dtc_dict["Type"] == "ProductML:DTC:Window":
        _validate_window(dtc_dict, name)
    elif dtc_dict["Type"] == "ProductML:DTC:Streaming":
        _validate_streaming(dtc_dict, name)
    else:
        raise ValueError("Document is not a valid DTC")