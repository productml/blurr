"""
Usage:
  blurr validate <DTC>

Options:
  -h --help     Show this screen.
  --version     Show version.
"""
import sys

from docopt import docopt

from blurr.cli.cli import cli
from blurr.util.out import Out


def read_version():
    version_file = open("blurr/VERSION","r")
    version = version_file.readline()
    version_file.close()
    return version


def main():
    arguments = docopt(__doc__, version=read_version())
    result = cli(arguments, Out())
    sys.exit(result)


if __name__ == '__main__':
    main()
