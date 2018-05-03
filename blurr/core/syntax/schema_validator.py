import os
import re
import traceback
from typing import Dict

from yamale import yamale
from yamale.schema import Data
from yamale.validators import DefaultValidators, Validator
from yamale.validators.constraints import Constraint

from blurr.core.errors import GenericSchemaError
from blurr.core.type import Type

IDENTITY_VALIDATOR_REGEX = re.compile(r'^_|^run_|[^\S]')


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


class DimensionDataType(Validator):
    TAG = 'dimension_data_type'

    VALUES = ['integer', 'boolean', 'string']

    def _is_valid(self, value: str) -> bool:
        return value in self.VALUES

    def get_name(self) -> str:
        return 'Valid LabelField Type'


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

    ERROR_STRING_INVALID_PYTHON_EXPRESSION = '\'%s\' is an invalid python expression.'
    failure_reason = None

    def _is_valid(self, value: str) -> bool:
        value = str(value)
        try:
            compile(str(value), '<string>', 'eval')
            return True
        except Exception as err:
            traceback.print_exc(limit=0)
            self.failure_reason = self.ERROR_STRING_INVALID_PYTHON_EXPRESSION
        return False

    def get_name(self) -> str:
        return 'Expression'

    def fail(self, value):
        return self.failure_reason % value


VALIDATORS = {
    **DefaultValidators.copy(),
    DataType.TAG: DataType,
    DimensionDataType.TAG: DimensionDataType,
    Identifier.TAG: Identifier,
    Expression.TAG: Expression,
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
        raise GenericSchemaError(str(e))


def _validate_streaming(dtc_dict: Dict, name: str) -> None:
    try:
        STREAMING_SCHEMA.validate(Data(dtc_dict, name))
    except ValueError as e:
        raise GenericSchemaError(str(e))


def is_window_dtc(dtc_dict: Dict) -> bool:
    return Type.is_type_equal(dtc_dict.get('Type', ''), Type.BLURR_TRANSFORM_WINDOW)


def is_streaming_dtc(dtc_dict: Dict) -> bool:
    return Type.is_type_equal(dtc_dict.get('Type', ''), Type.BLURR_TRANSFORM_STREAMING)


def validate(dtc_dict: Dict, name='dtc') -> None:
    if is_window_dtc(dtc_dict):
        _validate_window(dtc_dict, name)
    elif is_streaming_dtc(dtc_dict):
        _validate_streaming(dtc_dict, name)
    else:
        raise GenericSchemaError('Document has an invalid DTC \'Type\' {}.'.format(
            dtc_dict.get('Type', '')))
