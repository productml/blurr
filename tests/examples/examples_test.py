import pytest

from blurr.cli.cli import cli
from blurr.cli.validate import validate_file

# TODO Window BTS validation requires the streaming BTS to be in context
# Rethink validation in parameterized form


@pytest.mark.parametrize('input_bts', [
    'docs/examples/frequently-bought-together/fbt-streaming-bts.yml',
    'docs/examples/frequently-bought-together/fbt-window-bts.yml',
    'docs/examples/offer-ai/offer-ai-streaming-bts.yml',
    'docs/examples/offer-ai/offer-ai-window-bts.yml',
    'docs/examples/tutorial/tutorial1-streaming-bts.yml',
    'docs/examples/tutorial/tutorial2-streaming-bts.yml',
    'docs/examples/tutorial/tutorial2-window-bts.yml'
])
def test_example_bts_valid(input_bts):
    assert validate_file(input_bts) == 0


def get_file_text(file_path) -> str:
    with open(file_path, "r") as f:
        return f.read()


@pytest.mark.parametrize(
    'input_files, output_file',
    [(('docs/examples/tutorial/tutorial1-streaming-bts.yml', None,
       'docs/examples/tutorial/tutorial1-data.log'), "docs/examples/tutorial/tutorial1-output.log"),
     (('docs/examples/tutorial/tutorial2-streaming-bts.yml',
       'docs/examples/tutorial/tutorial2-window-bts.yml',
       'docs/examples/tutorial/tutorial2-data.log'), "docs/examples/tutorial/tutorial2-output.log")]
)
def test_example_bts_output(input_files, output_file, capsys):
    stream_bts_file, window_bts_file, raw_json_file = input_files
    assert cli({
        'transform': True,
        'validate': False,
        '--streaming-bts': stream_bts_file,
        '--window-bts': window_bts_file,
        '--source': raw_json_file,
        '--runner': 'local',
        '--data-processor': None,
        '<raw-json-files>': None,
    }) == 0
    out, err = capsys.readouterr()
    assert out.splitlines() == get_file_text(output_file).splitlines()
