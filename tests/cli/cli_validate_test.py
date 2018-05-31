from typing import List, Set

import yaml
from pytest import raises, mark

from blurr.cli.cli import cli
from blurr.cli.validate import validate
from blurr.core.aggregate import AggregateSchema
from blurr.core.anchor import AnchorSchema
from blurr.core.base import BaseSchema
from blurr.core.errors import SchemaError, BaseSchemaError, InvalidValueError, RequiredAttributeError, \
    InvalidNumberError, InvalidTypeError, InvalidExpressionError, InvalidIdentifierError
from blurr.core.transformer import TransformerSchema
from blurr.core.transformer_streaming import StreamingTransformerSchema
from blurr.core.type import Type


def run_command(dtc_files: List[str]) -> int:
    arguments = {
        'transform': False,
        'validate': True,
        'package-spark': False,
        '<DTC>': ['tests/core/syntax/dtcs/' + dtc_file for dtc_file in dtc_files]
    }
    return cli(arguments)


def get_running_validation_str(file_name: str) -> str:
    return 'Running validation on tests/core/syntax/dtcs/' + file_name


def test_valid_dtc(capsys):
    code = run_command(['valid_basic_streaming.yml'])
    out, err = capsys.readouterr()
    assert code == 0
    assert 'Document is valid' in out
    assert err == ''


def test_invalid_yaml(capsys):
    code = run_command(['invalid_yaml.yml'])
    out, err = capsys.readouterr()
    assert code == 1
    assert get_running_validation_str('invalid_yaml.yml') in out
    assert 'Invalid yaml' in err


def test_multiple_dtc_files(capsys):
    code = run_command(['valid_basic_streaming.yml', 'invalid_yaml.yml'])
    out, err = capsys.readouterr()
    assert code == 1
    assert get_running_validation_str('invalid_yaml.yml') in out
    assert 'Document is valid' in out
    assert get_running_validation_str('valid_basic_streaming.yml') in out
    assert 'Invalid yaml' in err


def test_invalid_dtc(capsys):
    code = run_command(['invalid_wrong_version.yml'])
    out, err = capsys.readouterr()
    assert code == 1
    assert 'Attribute `Version` under `example_name` must have one of the following values: 2018-03-01' in err
    assert get_running_validation_str('invalid_wrong_version.yml') in out


@mark.parametrize("file,errors", [
    ('invalid_wrong_version.yml',
     {InvalidValueError('example_name', {}, TransformerSchema.ATTRIBUTE_VERSION, {'2018-03-01'})}),
    ('invalid_missing_time.yml',
     {RequiredAttributeError('example_name', {}, StreamingTransformerSchema.ATTRIBUTE_TIME)}),
    ('invalid_string_instead_integer.yml',
     {InvalidNumberError('example_name.anchor', {}, AnchorSchema.ATTRIBUTE_MAX, int, 1)}),
    ('invalid_non_existing_data_type.yml',
     {InvalidTypeError('example_name.user.user_id', {}, BaseSchema.ATTRIBUTE_TYPE,
                       InvalidTypeError.Reason.TYPE_NOT_DEFINED)}),
    ('invalid_incorrect_expression.yml',
     {InvalidExpressionError('example_name', {}, BaseSchema.ATTRIBUTE_WHEN, None)}),
    ('invalid_set_expression.yml',
     {InvalidExpressionError('example_name', {}, BaseSchema.ATTRIBUTE_WHEN, None)}),
    ('invalid_aggregate_has_no_fields.yml',
     {RequiredAttributeError('example_name.user', {}, AggregateSchema.ATTRIBUTE_FIELDS)}),
    ('invalid_field_name.yml',
     {InvalidIdentifierError('example_name.user._name', {}, BaseSchema.ATTRIBUTE_NAME,
                             InvalidIdentifierError.Reason.STARTS_WITH_UNDERSCORE)}),
    ('invalid_dtc_type.yml',
     {InvalidTypeError('ProductMLExample', {}, BaseSchema.ATTRIBUTE_TYPE,
                       InvalidTypeError.Reason.TYPE_NOT_DEFINED)}),
    ('invalid_dimension_type.yml',
     {InvalidValueError('example_name.user.value', {}, BaseSchema.ATTRIBUTE_TYPE,
                        {Type.INTEGER.value, Type.STRING.value})}),
])
def test_invalid_schema(file: str, errors: Set[BaseSchemaError]) -> None:
    with raises(SchemaError) as err:
        dtc_dict = yaml.safe_load(open('tests/cli/dtcs/' + file, 'r', encoding='utf-8'))
        validate(dtc_dict)

    assert errors.issubset(set(err.value.errors.errors))
