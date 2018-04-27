from typing import Dict, Any

import yaml
from pytest import raises, fixture

from blurr.core.errors import InvalidSchemaError, RequiredAttributeError
from blurr.core.validator import validate_schema_basics, validate_required, validate_identifier


@fixture(scope='session')
def invalid_spec() -> Dict[str, Any]:
    return yaml.load('''
            Identity1: _illegal_identity
            Identity2: some space
            Identity3: contains*&^&^&*characters   
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


def test_validate_required_missing_attributes_raises_exceptions(invalid_spec):
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


def test_validate_identity_valid(valid_spec):
    assert validate_identifier('test', valid_spec, 'Name') is None


def test_validate_identity_invalid_raises_exceptions(invalid_spec):
    with raises(
            InvalidSchemaError,
            match='`Identity1: _illegal_identity` in section `test` is invalid. `Identity1` must '
            'not start with `_` and must be a python valid variable name.',
            message='Identity value starting with `_` did not raise exception'):
        validate_identifier('test', invalid_spec, 'Identity1')

        with raises(
                InvalidSchemaError,
                match='`Identity2: _illegal_identity` in section `test` is invalid. `Identity2` must '
                'not start with `_` and must be a python valid variable name.',
                message='Identity containing spaces did not raise exception'):
            validate_identifier('test', invalid_spec, 'Identity2')

        with raises(
                InvalidSchemaError,
                match='`Identity3: _illegal_identity` in section `test` is invalid. `Identity3` '
                'must not start with `_` and must be a python valid variable name.',
                message='Identity containing invalid characters did not raise exception'):
            validate_identifier('test', invalid_spec, 'Identity3')


def test_validate_schema_basics_empty_attributes_raises_exception():
    spec = yaml.load('''
        Name: ''
        Type: ''
        ''')
    with raises(
            InvalidSchemaError,
            match='`Name:`, `Type:` in section `test` cannot have an empty value.'):
        validate_schema_basics('test', spec)


def test_validate_schema_basics_missing_name_type_raises_exception(invalid_spec):
    with raises(InvalidSchemaError, match='`Name:`, `Type:` missing in section `test`'):
        validate_schema_basics('test', invalid_spec)


def test_validate_schema_basics_invalid_name_raises_exception():
    spec = yaml.load('''
        Name: _some /,
        Type: 'integer'
        ''')
    with raises(
            InvalidSchemaError,
            match='`Name: _some /,` in section `test` is invalid. `Name` must '
            'not start with `_` and must be a python valid variable name.'):
        validate_schema_basics('test', spec)
