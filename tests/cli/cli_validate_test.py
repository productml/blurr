from pytest import fixture

from blurr.cli.cli import cli
from tests.util.out_stub import OutStub


def run_command(dtc, out):
    arguments = {"validate": True, "<DTC>": 'tests/core/syntax/dtcs/' + dtc}
    return cli(arguments, out)


@fixture
def out():
    return OutStub()


def test_valid_dtc(out):
    code = run_command('valid_basic_streaming.yml', out)
    assert code == 0
    assert out.stdout == "document is valid\n"
    assert out.stderr == ""


def test_invalid_yaml(out):
    code = run_command('invalid_yaml.yml', out)
    assert code == 1
    assert out.stdout == ""
    assert out.stderr == "invalid yaml\n"


def test_invalid_dtc(out, snapshot):
    code = run_command('invalid_wrong_version.yml', out)
    assert code == 1
    assert out.stdout == ""
    snapshot.assert_match(out.stderr)
