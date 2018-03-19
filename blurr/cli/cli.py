from blurr.cli.validate import validate_command
from blurr.util.out import Out


def cli(arguments: list, out: Out) -> int:
    if arguments['validate']:
        return validate_command(arguments['<DTC>'], out)
