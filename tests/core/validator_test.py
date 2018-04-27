from typing import Dict, Any

import yaml
from pytest import raises, fixture

from blurr.core.errors import InvalidSchemaError, RequiredAttributeError, InvalidIdentifierError, EmptyAttributeError
from blurr.core.validator import validate_schema_basics, validate_required, validate_identifier
from os import linesep
from collections import Counter

@fixture(scope='session')
def invalid_spec() -> Dict[str, Any]:
    return yaml.load('''
            Identity1: _illegal_identity
            Identity2: some space
            Value: 2  
            ''')


@fixture(scope='session')
def valid_spec() -> Dict[str, Any]:
    return yaml.load('''
            Name: valid_name
            Type: integer
            When: True=True
            ''')


def test_validate_required_valid(valid_spec):
    assert validate_required('test', valid_spec, 'Name', 'Type') is None


def test_validate_required_missing_attributes(invalid_spec):
    error = validate_required('test', invalid_spec, 'Name', 'Type')

    assert isinstance(error, InvalidSchemaError)
    assert len(error.errors) == 2, 'Does not contain the correct number of nested RequiredAttributeErrors'
    assert error.fully_qualified_name == 'test', 'Fully qualified name not set in the error object'
    assert error.spec == invalid_spec, 'Spec is not set in the error object'

    name_error: RequiredAttributeError = error.errors[0]
    assert isinstance(name_error, RequiredAttributeError)
    assert name_error.fully_qualified_name == 'test', 'Fully qualified name not set in the error object'
    assert name_error.spec == invalid_spec, 'Spec is not set in the error object'
    assert name_error.attribute == 'Name'

    with raises(RequiredAttributeError, match='Attribute `Name` must be present under `test`.',
                message='Error message did not match expected pattern'):
        raise name_error

    with raises(InvalidSchemaError, match='Attribute `Name` must be present under `test`.' + linesep +
                                          'Attribute `Type` must be present under `test`.',
                message='Error message did not match expected pattern'):
        raise error


def test_validate_identity_valid(valid_spec):
    assert validate_identifier('test', valid_spec, 'Name') is None


def test_validate_identity_with_underscore(invalid_spec):

    error = validate_identifier('test', invalid_spec, 'Identity1')

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

    error = validate_identifier('test', invalid_spec, 'Identity2')
    assert error.reason == InvalidIdentifierError.Reason.INVALID_PYTHON_IDENTIFIER

    with raises(
            InvalidIdentifierError,
            match='`Identity2: some space` in section `test` is invalid. '
                  'Identifiers must be valid Python identifiers.',
            message='Message does not correctly reflect the reason'):
        raise error


def test_validate_schema_basics_empty_attributes_raises_exception():
    spec = yaml.load('''
        Name: ''
        Type: ''
        ''')

    error = validate_schema_basics('test', spec)
    assert isinstance(error, InvalidSchemaError)
    assert len(error.errors) == 5

    error_counts = Counter(list(map(lambda x: type(x), error.errors)))
    assert error_counts[EmptyAttributeError] == 2
    assert error_counts[RequiredAttributeError] == 2
    assert error_counts[InvalidIdentifierError] == 1


def test_validate_schema_basics_missing_name_type_raises_exception(invalid_spec):
    error = validate_schema_basics('test', invalid_spec)
    assert isinstance(error, InvalidSchemaError)
    assert len(error.errors) == 2
    assert isinstance(error.errors[0], RequiredAttributeError)
    assert isinstance(error.errors[1], RequiredAttributeError)


def test_validate_schema_basics_invalid_name_raises_exception():
    spec = yaml.load('''
        Name: _some /,
        Type: 'integer'
        ''')

    error = validate_schema_basics('test', spec)
    assert isinstance(error, InvalidSchemaError)
    assert len(error.errors) == 1
    assert isinstance(error.errors[0], InvalidIdentifierError)
