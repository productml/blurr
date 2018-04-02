from typing import Any
from unittest import mock

import os

from blurr.cli.util import get_yml_files, get_stream_window_dtc_files


def override_open(filename: str) -> Any:
    if filename == 'invalid.yml':
        content = "Type: ABC"
    elif filename == 'stream1.yml':
        content = 'Type: ProductML:DTC:Streaming'
    elif filename == 'stream2.yml':
        content = 'Type: ProductML:DTC:Streaming'
    elif filename == 'window1.yml':
        content = 'Type: ProductML:DTC:Window'
    elif filename == 'window2.yml':
        content = 'Type: ProductML:DTC:Window'
    else:
        raise FileNotFoundError(filename)
    file_object = mock.mock_open(read_data=content).return_value
    file_object.__iter__.return_value = content.splitlines(True)
    return file_object


def override_listdir(path: str) -> Any:
    if path == '.':
        content = []
    elif path == 'path1':
        content = ['file1.yml', 'file2.csv', 'file3.yaml', 'file4yml']
    elif path == 'path/inner_path':
        content = ['file1.yml']
    else:
        raise FileNotFoundError(path)

    return content


@mock.patch('os.listdir', new=override_listdir)
def test_get_yml_files():
    assert get_yml_files() == []
    assert sorted(get_yml_files('path1')) == sorted([
        os.path.join('path1', 'file1.yml'),
        os.path.join('path1', 'file3.yaml')
    ])
    assert get_yml_files('path/inner_path') == [
        os.path.join('path/inner_path', 'file1.yml')
    ]


@mock.patch('builtins.open', new=override_open)
def test_get_stream_window_dtc_files_bad_files():
    assert get_stream_window_dtc_files([]) == (None, None)
    assert get_stream_window_dtc_files(['invalid.yml']) == (None, None)


@mock.patch('builtins.open', new=override_open)
def test_get_stream_window_dtc_files_missing_stream():
    assert get_stream_window_dtc_files(
        ['invalid.yml', 'stream1.yml']) == ('stream1.yml', None)


@mock.patch('builtins.open', new=override_open)
def test_get_stream_window_dtc_files_missing_window():
    assert get_stream_window_dtc_files(
        ['invalid.yml', 'window2.yml']) == (None, 'window2.yml')


@mock.patch('builtins.open', new=override_open)
def test_get_stream_window_dtc_files_valid():
    assert get_stream_window_dtc_files(
        ['stream1.yml', 'stream2.yml', 'window1.yml']) == ('stream1.yml',
                                                           'window1.yml')
