import yaml
from pytest import raises, mark

from blurr.core.errors import GenericSchemaError, SchemaError
from blurr.core.syntax.schema_validator import validate, Identifier, Expression


def test_valid_identifier():
    validator = Identifier(None, None)
    assert validator._is_valid('valid_string_with_numbers_and_!@£$$%%^&*()')


def test_invalid_identifiers():
    validator = Identifier(None, None)
    assert not validator._is_valid('identifier with spaces')
    assert not validator._is_valid('identifier_with\t_tabs')
    assert not validator._is_valid('identifier_with\n_new_lines')
    assert not validator._is_valid('_identifier_with_underscore')
    assert not validator._is_valid('run_identifier_run')


def test_valid_expression():
    validator = Expression(None, None)
    assert validator._is_valid("is_valid_python('1 // 2')")
    assert validator._is_valid('a==b')
    assert validator._is_valid('a == c')
    assert validator._is_valid('a==b == c')
    assert validator._is_valid('a!=b')
    assert validator._is_valid('a != c')
    assert validator._is_valid('a!=b != c')


def test_invalid_expression():
    validator = Expression(None, None)
    assert not validator._is_valid('regular text')
    assert not validator._is_valid('is_invalid_python(1 /// 2)')
    assert not validator._is_valid('a= b')
    assert not validator._is_valid('a!=b = ')
    assert not validator._is_valid(' =a')
    assert not validator._is_valid('b =')
    assert not validator._is_valid('b &= c')
    assert not validator._is_valid('b += c')
    assert not validator._is_valid('b |= c')
    assert not validator._is_valid('b /= c')


def load_example(file):
    return yaml.safe_load(open('tests/core/syntax/dtcs/' + file))


def test_validation_errors_contain_dtc_name_and_schema_location():
    with raises(GenericSchemaError, match='Error validating data dtc_name with schema'):
        dtc_dict = load_example('invalid_wrong_version.yml')
        validate(dtc_dict, 'dtc_name')


def test_valid_basic_streaming_dtc():
    dtc_dict = load_example('valid_basic_streaming.yml')
    validate(dtc_dict)


def test_valid_basic_window_dtc():
    dtc_dict = load_example('valid_basic_window.yml')
    validate(dtc_dict)
