from typing import Dict, Any

import yaml
from pytest import raises, fixture

from blurr.core.errors import RequiredAttributeError, InvalidIdentifierError, EmptyAttributeError, InvalidValueError
from blurr.core.validator import validate_required_attributes, validate_number_attribute, \
    validate_python_identifier_attributes, ATTRIBUTE_INTERNAL, validate_empty_attributes, validate_enum_attribute


@fixture
def invalid_spec() -> Dict[str, Any]:
    return yaml.load('''
            Identity1: _illegal_identity
            Identity2: some space
            Identity3: run_reserved
            Value: 2  
            ''')


@fixture
def valid_spec() -> Dict[str, Any]:
    return yaml.load('''
            Name: valid_name
            Type: integer
            When: True=True
            ''')


def test_validate_required_valid(valid_spec):
    assert not validate_required_attributes('test', valid_spec, 'Name', 'Type')


def test_validate_required_missing_attributes(invalid_spec):
    error_collection = validate_required_attributes('test', invalid_spec, 'Name', 'Type')
    assert len(error_collection) == 2

    error = error_collection[0]
    assert isinstance(error, RequiredAttributeError)
    assert error.fully_qualified_name == 'test', 'Fully qualified name not set in the error object'
    assert error.spec == invalid_spec, 'Spec is not set in the error object'
    assert error.attribute == 'Name'

    with raises(
            RequiredAttributeError,
            match='Attribute `Name` must be present under `test`.',
            message='Error message did not match expected pattern'):
        raise error


def test_validate_python_identifier_attributes_valid(valid_spec):
    assert not validate_python_identifier_attributes('test', valid_spec, 'Name')


def test_validate_python_identifier_attributes_internal(invalid_spec):
    invalid_spec[ATTRIBUTE_INTERNAL] = True
    assert not validate_python_identifier_attributes('test', invalid_spec, 'Identity1', 'Identity2',
                                                     'Identity3')


def test_validate_python_identifier_attributes_with_error_conditions(invalid_spec):
    error_collection = validate_python_identifier_attributes('test', invalid_spec, 'Identity1',
                                                             'Identity2', 'Identity3')
    assert len(error_collection) == 3

    error = error_collection[0]
    assert isinstance(error, InvalidIdentifierError)
    assert error.fully_qualified_name == 'test', 'Fully qualified name not set in the error object'
    assert error.spec == invalid_spec, 'Spec is not set in the error object'
    assert error.attribute == 'Identity1'
    assert error.reason == InvalidIdentifierError.Reason.STARTS_WITH_UNDERSCORE

    with raises(
            InvalidIdentifierError,
            match='`Identity1: _illegal_identity` in section `test` is invalid. '
            'Identifiers starting with underscore `_` are reserved.',
            message='Message does not correctly reflect the reason'):
        raise error

    error = error_collection[1]
    assert error.reason == InvalidIdentifierError.Reason.INVALID_PYTHON_IDENTIFIER

    with raises(
            InvalidIdentifierError,
            match='`Identity2: some space` in section `test` is invalid. '
            'Identifiers must be valid Python identifiers.',
            message='Message does not correctly reflect the reason'):
        raise error

    error = error_collection[2]
    assert error.reason == InvalidIdentifierError.Reason.STARTS_WITH_RUN

    with raises(
            InvalidIdentifierError,
            match='`Identity3: run_reserved` in section `test` is invalid. '
            'Identifiers starting with `run_` are reserved.',
            message='Message does not correctly reflect the reason'):
        raise error


def test_validate_empty_attributes():
    spec = yaml.load('''
        Name: ''
        Type: ''
        ''')

    errors = validate_empty_attributes('test', spec, *spec.keys())
    assert len(errors) == 2

    with raises(
            EmptyAttributeError,
            match='Attribute `Name` under `test` cannot be left empty.',
            message='Error message did not match expected pattern'):
        raise errors[0]

 
def test_validate_enum_attribute(invalid_spec):
    error = validate_enum_attribute('test', invalid_spec, 'Identity3',
                                    {'candidate1', 'candidate2', 'candidate3'})
    assert error

    with raises(
            InvalidValueError,
            match=
            'Attribute `Identity3` under `test` must have one of the following values: candidate1 | candidate2 | candidate3',
            message='Error message did not match expected pattern'):
        raise error


def test_validate_number_attribute():
    assert validate_number_attribute('test', {'number': -3}, 'number', int, 0, 3)
    assert validate_number_attribute('test', {'number': -3}, 'number', int, -3, -3) is None
    assert validate_number_attribute('test', {'number': 3}, 'number', int, 0) is None
    assert validate_number_attribute('test', {'number': 3}, 'number', int, None, 0)
    assert validate_number_attribute('test', {'number': 3}, 'number', int, None, 5) is None
