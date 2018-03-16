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


def main():
    arguments = docopt(__doc__, version='pre-alpha')
    result = cli(arguments, Out())
    sys.exit(result)


if __name__ == '__main__':
    main()
