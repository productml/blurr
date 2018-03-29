"""
Usage:
  blurr validate <DTC>

Options:
  -h --help     Show this screen.
  --version     Show version.
"""
import sys

import os
from docopt import docopt

from blurr.cli.cli import cli
from blurr.util.out import Out

VERSION_PATH = "blurr/VERSION"


def read_version() -> str:
    if os.path.exists(VERSION_PATH) and os.path.isfile(VERSION_PATH):
        version_file = open("blurr/VERSION", "r")
        version = version_file.readline()
        version_file.close()
        return version
    else:
        return "LOCAL"


def main() -> None:
    arguments = docopt(__doc__, version=read_version())
    result = cli(arguments, Out())
    sys.exit(result)


if __name__ == '__main__':
    main()
