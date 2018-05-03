import pytest
import json
from typing import Any

from blurr.cli.validate import validate_file
from blurr.cli.cli import cli


@pytest.mark.parametrize('input_dtc', [
    'docs/examples/frequently-bought-together/fbt-streaming-dtc.yml',
    'docs/examples/frequently-bought-together/fbt-window-dtc.yml',
    'docs/examples/offer-ai/offer-ai-streaming-dtc.yml',
    'docs/examples/offer-ai/offer-ai-window-dtc.yml',
    'docs/examples/tutorial/tutorial1-streaming-dtc.yml',
    'docs/examples/tutorial/tutorial2-streaming-dtc.yml',
    'docs/examples/tutorial/tutorial2-window-dtc.yml'
])
def test_example_dtc_valid(input_dtc):
    assert validate_file(input_dtc) == 0


def get_file_text(file_path) -> str:
    with open(file_path, "r") as f:
        return f.read()


@pytest.mark.parametrize(
    'input_files, output_file',
    [(('docs/examples/tutorial/tutorial1-streaming-dtc.yml', None,
       'docs/examples/tutorial/tutorial1-data.log'), "docs/examples/tutorial/tutorial1-output.log"),
     (('docs/examples/tutorial/tutorial2-streaming-dtc.yml',
       'docs/examples/tutorial/tutorial2-window-dtc.yml',
       'docs/examples/tutorial/tutorial2-data.log'), "docs/examples/tutorial/tutorial2-output.log")]
)
def test_example_dtc_output(input_files, output_file, capsys):
    stream_dtc_file, window_dtc_file, raw_json_file = input_files
    assert cli({
        'transform': True,
        'validate': False,
        '--streaming-dtc': stream_dtc_file,
        '--window-dtc': window_dtc_file,
        '--source': raw_json_file,
        '--runner': 'local',
        '--data-processor': None,
        '<raw-json-files>': None,
    }) == 0
    out, err = capsys.readouterr()
    assert out.splitlines() == get_file_text(output_file).splitlines()
