"""
Usage:
    blurr validate [<DTC> ...]
    blurr transform [--streaming-dtc=<dtc-file>] [--window-dtc=<dtc-file>] (--source=<raw-json-files> | <raw-json-files>)
    blurr -h | --help

Commands:
    validate        Runs syntax validation on the list of DTC files provided. If
                    no files are provided then all *.dtc files in the current
                    directory are validated.
    transform       Runs blurr to process the given raw log file. This command
                    can be run with the following combinations:
                    1. No DTC provided - The current directory is searched for
                    DTCs. First streaming and the first window DTC are used.
                    2. Only streaming DTC given - Transform outputs the result of
                    applying the DTC on the raw data file.
                    3. Both streaming and window DTC are provided - Transform
                    outputs the final result of applying the streaming and window
                    DTC on the raw data file.

Options:
    -h --help                   Show this screen.
    --version                   Show version.
    --streaming-dtc=<dtc-file>  Streaming DTC file to use.
    --window-dtc=<dtc-file>     Window DTC file to use.
    --source=<raw-json-files>   List of source files separated by comma
"""
import sys

import os
from docopt import docopt

from blurr.cli.cli import cli
from blurr.cli.out import Out

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
VERSION_PATH = os.path.join(PACKAGE_DIR, 'VERSION')


def read_version():
    if os.path.exists(VERSION_PATH) and os.path.isfile(VERSION_PATH):
        version_file = open(VERSION_PATH, 'r')
        version = version_file.readline()
        version_file.close()
        return version
    else:
        return 'LOCAL'


def main():
    arguments = docopt(__doc__, version=read_version())
    result = cli(arguments, Out())
    sys.exit(result)


if __name__ == '__main__':
    main()
