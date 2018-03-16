from pytest import fixture

from blurr.cli.cli import cli
from blurr.util.out import OutStub


def run_command(dtc, out):
    arguments = {"validate": True, "<DTC>": 'tests/cli/dtcs/' + dtc}
    return cli(arguments, out)


@fixture
def out():
    return OutStub()


def test_valid_dtc(out):
    code = run_command('valid_dtc.yml', out)
    assert code == 0
    assert out.stdout == "document is valid\n"
    assert out.stderr == ""


def test_invalid_yaml(out):
    code = run_command('invalid_yaml.yml', out)
    assert code == 1
    assert out.stdout == ""
    assert out.stderr == "invalid yaml\n"


def test_invalid_dtc(out, snapshot):
    code = run_command('invalid_dtc.yml', out)
    assert code == 1
    assert out.stdout == ""
    snapshot.assert_match(out.stderr)
