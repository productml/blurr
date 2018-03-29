from typing import Any, Dict

import yaml
from pytest import raises

from blurr.core.errors import InvalidSchemaError
from blurr.core.syntax.schema_validator import is_identifier, is_expression, \
    validate


def test_valid_identifier() -> None:
    assert is_identifier('valid_string_with_numbers_and_!@£$$%%^&*()')


def test_invalid_identifiers() -> None:
    assert not is_identifier('identifier with spaces')
    assert not is_identifier('identifier_with\t_tabs')
    assert not is_identifier('identifier_with\n_new_lines')


def test_valid_expression() -> None:
    assert is_expression("is_valid_python('1 // 2')")


def test_invalid_expression() -> None:
    assert not is_expression('regular text')
    assert not is_expression('is_invalid_python(1 /// 2)')


def load_example(file: str) -> Dict[str, Any]:
    return yaml.safe_load(open('tests/core/syntax/dtcs/' + file))


def test_validation_errors_contain_dtc_name_and_schema_location() -> None:
    with raises(InvalidSchemaError) as err:
        dtc_dict = load_example('invalid_wrong_version.yml')
        validate(dtc_dict, 'dtc_name')
    assert 'Error validating data dtc_name with schema blurr/core/syntax/dtc_streaming_schema.yml' in str(
        err.value)


def test_valid_basic_streaming_dtc() -> None:
    dtc_dict = load_example('valid_basic_streaming.yml')
    validate(dtc_dict)


def test_valid_basic_window_dtc() -> None:
    dtc_dict = load_example('valid_basic_window.yml')
    validate(dtc_dict)


def test_invalid_wrong_version() -> None:
    with raises(InvalidSchemaError) as err:
        dtc_dict = load_example('invalid_wrong_version.yml')
        validate(dtc_dict)
    assert "Version: '2088-03-01' not in ('2018-03-01',)" in str(err.value)


def test_invalid_string_instead_of_integer() -> None:
    with raises(InvalidSchemaError) as err:
        dtc_dict = load_example('invalid_string_instead_integer.yml')
        validate(dtc_dict)
    assert "Anchor.Max: 'one' is not a int." in str(err.value)


def test_invalid_non_existing_data_type() -> None:
    with raises(InvalidSchemaError) as err:
        dtc_dict = load_example('invalid_non_existing_data_type.yml')
        validate(dtc_dict)
    assert "Type: 'foo' is not a DTC Valid Data Type." in str(err.value)


def test_invalid_incorrect_expression() -> None:
    with raises(InvalidSchemaError) as err:
        dtc_dict = load_example('invalid_incorrect_expression.yml')
        validate(dtc_dict)
    assert "When: 'x == senor roy' is not a Expression." in str(err.value)


def test_invalid_datagroup_has_no_fields() -> None:
    with raises(InvalidSchemaError) as err:
        dtc_dict = load_example('invalid_datagroup_has_no_fields.yml')
        validate(dtc_dict)
    assert 'DataGroups.0.Fields: Required field missing' in str(err.value)
