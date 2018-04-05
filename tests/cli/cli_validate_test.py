import logging
from typing import List

from blurr.cli.cli import cli


def run_command(dtc_files: List[str]) -> int:
    arguments = {
        'validate': True,
        '<DTC>': ['tests/core/syntax/dtcs/' + dtc_file for dtc_file in dtc_files]
    }
    return cli(arguments)


def get_running_validation_str(file_name: str) -> str:
    return 'Running syntax validation on tests/core/syntax/dtcs/' + file_name


def test_valid_dtc(caplog):
    caplog.set_level(logging.INFO)
    code = run_command(['valid_basic_streaming.yml'])
    assert code == 0
    assert 'Document is valid' in caplog.text


def test_invalid_yaml(caplog):
    caplog.set_level(logging.INFO)
    code = run_command(['invalid_yaml.yml'])
    assert code == 1
    assert get_running_validation_str('invalid_yaml.yml') in caplog.text
    assert 'Invalid yaml' in caplog.text


def test_multiple_dtc_files(caplog):
    caplog.set_level(logging.INFO)
    code = run_command(['valid_basic_streaming.yml', 'invalid_yaml.yml'])
    assert code == 1
    assert get_running_validation_str('invalid_yaml.yml') in caplog.text
    assert 'Document is valid' in caplog.text
    assert get_running_validation_str('valid_basic_streaming.yml') in caplog.text
    assert 'Invalid yaml' in caplog.text


def test_invalid_dtc(caplog):
    caplog.set_level(logging.INFO)
    code = run_command(['invalid_wrong_version.yml'])
    assert code == 1
    assert 'Error validating data dtc with schema' in caplog.text
    assert 'Version: \'2088-03-01\' not in (\'2018-03-01\',)' in caplog.text
    assert get_running_validation_str('invalid_wrong_version.yml') in caplog.text
