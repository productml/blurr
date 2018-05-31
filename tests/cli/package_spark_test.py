import os
import tempfile
import uuid

from _pytest.capture import CaptureFixture

from blurr.cli.cli import cli

TMP = tempfile.mkdtemp()


def run_command(example: str, target: str) -> int:
    arguments = {
        'transform': False,
        'validate': False,
        'package-spark': True,
        '--source-dir': 'tests/cli/package_spark_examples/' + example,
        '--target': target
    }
    return cli(arguments)


def generate_target_filename():
    return os.path.join(TMP, str(uuid.uuid4()) + '.zip')

def test_app_with_requirements_txt_generates_spark_app(capsys: CaptureFixture) -> None:
    target_file = generate_target_filename()
    assert run_command('app_with_requirements_txt', target_file) == 0
    assert os.path.isfile(target_file), "spark app zipfile wasn't generated"

    out, err = capsys.readouterr()
    assert "spark app generated successfully:" in out
    assert err == ''


def test_app_with_no_requirements_txt_fails(capsys: CaptureFixture) -> None:
    target_file = generate_target_filename()
    assert run_command('app_with_no_requirements_txt', target_file) == 1
    assert not os.path.isfile(target_file), "spark app shouldn't have been generated"

    out, err = capsys.readouterr()
    assert "" in out
    assert "requirements.txt not found in " in err


def test_app_with_broken_requirements_txt_fails(capsys: CaptureFixture) -> None:
    target_file = generate_target_filename()
    assert run_command('app_with_broken_requirements_txt', target_file) == 1
    assert not os.path.isfile(target_file), "spark app shouldn't have been generated"

    out, err = capsys.readouterr()
    assert out == ""
    assert "there was an error processing " in err


def test_app_from_invalid_source_dir_fails(capsys: CaptureFixture) -> None:
    target_file = generate_target_filename()
    assert run_command("foo", target_file) == 1
    assert not os.path.isfile(target_file), "spark app shouldn't have been generated"

    out, err = capsys.readouterr()
    assert out == ""
    assert "foo is not a valid directory" in err
