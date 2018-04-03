import ast
from typing import Dict

import os
import re
from yamale import yamale
from yamale.schema import Data
from yamale.validators import DefaultValidators, Validator
from yamale.validators.constraints import Constraint

from blurr.core.errors import InvalidSchemaError


def is_expression(s: str) -> bool:
    try:
        ast.parse(s)
    except SyntaxError:
        return False
    except TypeError:
        return False
    return True


def is_identifier(s: str) -> bool:
    return len(re.findall(r'[^\S]', s)) == 0




class StringExclude(Constraint):
    keywords = {'exclude': list}
    fail = '\'%s\' is a reserved keyword.  Please try another.'

    def _is_valid(self, value):
        return value not in self.exclude

    def _fail(self, value):
        return self.fail % value


class DataType(Validator):
    TAG = 'data_type'

    VALUES = [
        'integer', 'boolean', 'string', 'datetime', 'float', 'map', 'list',
        'set'
    ]

    def _is_valid(self, value: str) -> bool:
        return value in self.VALUES

    def get_name(self) -> str:
        return 'DTC Valid Data Type'


class Identifier(Validator):
    TAG = 'identifier'
    constraints = [StringExclude]

    def _is_valid(self, value: str) -> bool:
        return is_identifier(value)

    def get_name(self) -> str:
        return 'Identifier'


class Expression(Validator):
    TAG = 'expression'

    def _is_valid(self, value: str) -> bool:
        return is_expression(str(value))

    def get_name(self) -> str:
        return 'Expression'


VALIDATORS = {
    **DefaultValidators.copy(), DataType.TAG: DataType,
    Identifier.TAG: Identifier,
    Expression.TAG: Expression
}

package_directory = os.path.dirname(os.path.abspath(__file__))
STREAMING_SCHEMA = yamale.make_schema(
    os.path.join(package_directory, 'dtc_streaming_schema.yml'),
    validators=VALIDATORS)

WINDOW_SCHEMA = yamale.make_schema(
    os.path.join(package_directory, 'dtc_window_schema.yml'),
    validators=VALIDATORS)


def _validate_window(dtc_dict: Dict, name: str) -> None:
    try:
        WINDOW_SCHEMA.validate(Data(dtc_dict, name))
    except ValueError as e:
        raise InvalidSchemaError(str(e))


def _validate_streaming(dtc_dict: Dict, name: str) -> None:
    try:
        STREAMING_SCHEMA.validate(Data(dtc_dict, name))
    except ValueError as e:
        raise InvalidSchemaError(str(e))


def is_window_dtc(dtc_dict: Dict) -> bool:
    return dtc_dict.get('Type', '').lower() == 'blurr:window'


def is_streaming_dtc(dtc_dict: Dict) -> bool:
    return dtc_dict.get('Type', '').lower() == 'blurr:streaming'


def validate(dtc_dict: Dict, name='dtc') -> None:
    if is_window_dtc(dtc_dict):
        _validate_window(dtc_dict, name)
    elif is_streaming_dtc(dtc_dict):
        _validate_streaming(dtc_dict, name)
    else:
        raise ValueError('Document is not a valid DTC')
