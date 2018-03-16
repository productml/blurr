"""
Usage:
  blurr validate <DTC>

Options:
  -h --help     Show this screen.
  --version     Show version.
"""
from docopt import docopt

from blurr.cli.cli import cli

if __name__ == '__main__':
    cli(docopt(__doc__, version='pre-alpha'))
