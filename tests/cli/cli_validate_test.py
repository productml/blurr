from typing import List

from blurr.cli.cli import cli


def run_command(dtc_files: List[str]) -> int:
    arguments = {
        'validate_schema_spec': True,
        '<DTC>': ['tests/core/syntax/dtcs/' + dtc_file for dtc_file in dtc_files]
    }
    return cli(arguments)


def get_running_validation_str(file_name: str) -> str:
    return 'Running syntax validation on tests/core/syntax/dtcs/' + file_name


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
    assert 'There was an error parsing the document' in err
    #assert 'Version: \'2088-03-01\' not in (\'2018-03-01\',)' in err
    assert get_running_validation_str('invalid_wrong_version.yml') in out
