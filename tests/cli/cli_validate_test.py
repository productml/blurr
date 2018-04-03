from typing import List

from pytest import fixture

from blurr.cli.cli import cli
from tests.cli.out_stub import OutStub


def run_command(dtc_files: List[str], out: OutStub) -> int:
    arguments = {
        'validate': True,
        '<DTC>': [
            'tests/core/syntax/dtcs/' + dtc_file for dtc_file in dtc_files
        ]
    }
    return cli(arguments, out)


def get_running_validation_str(file_name: str) -> str:
    return 'Running syntax validation on tests/core/syntax/dtcs/' + file_name


@fixture
def out():
    return OutStub()


def test_valid_dtc(out: OutStub):
    code = run_command(['valid_basic_streaming.yml'], out)
    assert code == 0
    assert 'document is valid' in out.stdout
    assert out.stderr == ''


def test_invalid_yaml(out):
    code = run_command(['invalid_yaml.yml'], out)
    assert code == 1
    assert get_running_validation_str('invalid_yaml.yml') in out.stdout
    assert 'invalid yaml' in out.stderr


def test_multiple_dtc_files(out):
    code = run_command(['valid_basic_streaming.yml', 'invalid_yaml.yml'], out)
    assert code == 1
    assert get_running_validation_str('invalid_yaml.yml') in out.stdout
    assert 'document is valid' in out.stdout
    assert get_running_validation_str(
        'valid_basic_streaming.yml') in out.stdout
    assert 'invalid yaml' in out.stderr


def test_invalid_dtc(out):
    code = run_command(['invalid_wrong_version.yml'], out)
    assert code == 1
    assert 'Error validating data dtc with schema' in out.stderr
    assert 'Version: \'2088-03-01\' not in (\'2018-03-01\',)' in out.stderr
    assert get_running_validation_str(
        'invalid_wrong_version.yml') in out.stdout
