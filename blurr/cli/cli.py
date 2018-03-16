from blurr.cli.validate import validate_command


def cli(arguments):
    if arguments["validate"]:
        validate_command(arguments["<DTC>"])


if __name__ == '__main__':
    print("jello!")
