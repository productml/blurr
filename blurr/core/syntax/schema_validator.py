import ast
import os
import re
from typing import Dict

from yamale import yamale
from yamale.schema import Data
from yamale.validators import DefaultValidators, Validator
from yamale.validators.constraints import Constraint

from blurr.core.errors import InvalidSchemaError

EQUAL_OPERATOR_EXISTS_REGEX = re.compile(r'(?:^|[^!=]+)=(?:[^=]+|$)')
IDENTITY_VALIDATOR_REGEX = re.compile(r'^_|[^\S]')


class StringExclude(Constraint):
    keywords = {'exclude': list}
    fail = '\'%s\' is a reserved keyword.  Please try another.'

    def _is_valid(self, value):
        return value not in self.exclude

    def _fail(self, value):
        return self.fail % value


class DataType(Validator):
    TAG = 'data_type'

    VALUES = ['integer', 'boolean', 'string', 'datetime', 'float', 'map', 'list', 'set']

    def _is_valid(self, value: str) -> bool:
        return value in self.VALUES

    def get_name(self) -> str:
        return 'DTC Valid Data Type'


class Identifier(Validator):
    TAG = 'identifier'
    constraints = [StringExclude]

    def _is_valid(self, value: str) -> bool:
        return not IDENTITY_VALIDATOR_REGEX.findall(value)

    def get_name(self) -> str:
        return 'Identifier'

    def fail(self, value):
        return '\'%s\' starts with _ or containing whitespace characters.' % value


class Expression(Validator):
    TAG = 'expression'

    ERROR_STRING_SET_NOT_ALLOWED = '\'%s\' sets value using `=`.'
    ERROR_STRING_INVALID_PYTHON_EXPRESSION = '\'%s\' is an invalid python expression.'
    failure_reason = None

    def _is_valid(self, value: str) -> bool:
        value = str(value)
        if EQUAL_OPERATOR_EXISTS_REGEX.findall(value):
            self.failure_reason = self.ERROR_STRING_SET_NOT_ALLOWED
            return False
        elif not self.is_valid_python_expression(value):
            self.failure_reason = self.ERROR_STRING_INVALID_PYTHON_EXPRESSION
            return False
        return True

    def get_name(self) -> str:
        return 'Expression'

    def fail(self, value):
        return self.failure_reason % value

    @staticmethod
    def is_valid_python_expression(expression):
        try:
            ast.parse(expression)
        except SyntaxError:
            return False
        except TypeError:
            return False
        return True


VALIDATORS = {
    **DefaultValidators.copy(), DataType.TAG: DataType,
    Identifier.TAG: Identifier,
    Expression.TAG: Expression
}

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
STREAMING_SCHEMA = yamale.make_schema(
    os.path.join(PACKAGE_DIR, 'dtc_streaming_schema.yml'), validators=VALIDATORS)

WINDOW_SCHEMA = yamale.make_schema(
    os.path.join(PACKAGE_DIR, 'dtc_window_schema.yml'), validators=VALIDATORS)


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
