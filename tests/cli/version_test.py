from typing import Any
from unittest import mock

from blurr.__main__ import read_version


# Mode is needed for the signature but is ignored in the function.
def override_open(filename: str, mode: str) -> Any:
    if filename == 'VALID_FILE':
        content = "1.0.0"
    elif filename == 'DEV_FILE':
        content = '1.0.0-dev'
    else:
        raise FileNotFoundError(filename)
    file_object = mock.mock_open(read_data=content).return_value
    file_object.__iter__.return_value = content.splitlines(True)
    return file_object


def override_isfile(filename: str) -> bool:
    if filename == 'VALID_FILE':
        return True
    elif filename == 'DEV_FILE':
        return True
    return False


def override_exists(filename: str) -> bool:
    if filename == 'VALID_FILE':
        return True
    elif filename == 'DEV_FILE':
        return True
    elif filename == 'DIR':
        return True
    return False


@mock.patch('builtins.open', new=override_open)
@mock.patch('os.path.isfile', new=override_isfile)
@mock.patch('os.path.exists', new=override_exists)
def test_version():
    assert read_version('VALID_FILE') == '1.0.0'
    assert read_version('DEV_FILE') == '1.0.0-dev'
    assert read_version('DIR') == 'LOCAL'
    assert read_version('UNKNOWN') == 'LOCAL'
