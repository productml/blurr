import yaml
from pytest import raises, mark

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
    with raises(InvalidSchemaError, match='Error validating data dtc_name with schema'):
        dtc_dict = load_example('invalid_wrong_version.yml')
        validate(dtc_dict, 'dtc_name')


def test_valid_basic_streaming_dtc():
    dtc_dict = load_example('valid_basic_streaming.yml')
    validate(dtc_dict)


def test_valid_basic_window_dtc():
    dtc_dict = load_example('valid_basic_window.yml')
    validate(dtc_dict)


@mark.parametrize(
    "test_file,err_string",
    [('invalid_wrong_version.yml', 'Version: \'2088-03-01\' not in \(\'2018-03-01\',\)'),
     ('invalid_missing_time.yml', 'Time: Required field missing'),
     ('invalid_string_instead_integer.yml', "Anchor.Max: 'one' is not a int."),
     ('invalid_non_existing_data_type.yml', "Type: 'foo' is not a DTC Valid Data Type."),
     ('invalid_incorrect_expression.yml',
      "When: 'x == senor roy' is an invalid python expression."),
     ('invalid_set_expression.yml', 'When: \'x = \'test\'\' is an invalid python expression'),
     ('invalid_datagroup_has_no_fields.yml', 'DataGroups.0.Fields: Required field missing'),
     ('invalid_field_name.yml', "Name: '_name' starts with _ or containing whitespace characters."),
     ('invalid_import.yml', 'Import.0.Module: Required field missing')])
def test_invalid_schema(test_file: str, err_string: str) -> None:
    with raises(InvalidSchemaError, match=err_string):
        validate(load_example(test_file))
