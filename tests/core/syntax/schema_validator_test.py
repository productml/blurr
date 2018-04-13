import yaml
from pytest import raises

from blurr.core.errors import InvalidSchemaError
from blurr.core.syntax.schema_validator import validate, Identifier, Expression


def test_valid_identifier():
    validator = Identifier(None, None)
    assert validator._is_valid('valid_string_with_numbers_and_!@£$$%%^&*()')


def test_invalid_identifiers():
    validator = Identifier(None, None)
    assert not validator._is_valid('identifier with spaces')
    assert not validator._is_valid('identifier_with\t_tabs')
    assert not validator._is_valid('identifier_with\n_new_lines')


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
    with raises(InvalidSchemaError, match='Error validating data dtc_name with schema') as err:
        dtc_dict = load_example('invalid_wrong_version.yml')
        validate(dtc_dict, 'dtc_name')


def test_valid_basic_streaming_dtc():
    dtc_dict = load_example('valid_basic_streaming.yml')
    validate(dtc_dict)


def test_valid_basic_window_dtc():
    dtc_dict = load_example('valid_basic_window.yml')
    validate(dtc_dict)


def test_invalid_wrong_version():
    with raises(InvalidSchemaError) as err:
        dtc_dict = load_example('invalid_wrong_version.yml')
        validate(dtc_dict)
    assert "Version: '2088-03-01' not in ('2018-03-01',)" in str(err.value)


def test_invalid_missing_time():
    with raises(InvalidSchemaError, match='Time: Required field missing') as err:
        dtc_dict = load_example('invalid_missing_time.yml')
        validate(dtc_dict)


def test_invalid_string_instead_of_integer():
    with raises(InvalidSchemaError, match="Anchor.Max: 'one' is not a int.") as err:
        dtc_dict = load_example('invalid_string_instead_integer.yml')
        validate(dtc_dict)


def test_invalid_non_existing_data_type():
    with raises(InvalidSchemaError, match="Type: 'foo' is not a DTC Valid Data Type.") as err:
        dtc_dict = load_example('invalid_non_existing_data_type.yml')
        validate(dtc_dict)


def test_invalid_incorrect_expression():
    with raises(
            InvalidSchemaError,
            match="When: 'x == senor roy' is an invalid python expression.") as err:
        dtc_dict = load_example('invalid_incorrect_expression.yml')
        validate(dtc_dict)


def test_invalid_aggregate_has_no_fields():
    with raises(InvalidSchemaError, match='Aggregates.0.Fields: Required field missing') as err:
        dtc_dict = load_example('invalid_aggregate_has_no_fields.yml')
        validate(dtc_dict)


def test_reserved_keywords_as_field_names_raises_invalid_schema_error():
    with raises(
            InvalidSchemaError,
            match="Name: '_name' starts with _ or containing whitespace characters.") as err:
        dtc_dict = load_example('invalid_field_name.yml')
        validate(dtc_dict)
