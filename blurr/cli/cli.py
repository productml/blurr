from blurr.cli.validate import validate_command


def cli(arguments, out):
    if arguments["validate"]:
        return validate_command(arguments["<DTC>"], out)