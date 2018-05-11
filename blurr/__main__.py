"""
Usage:
    blurr validate [--debug] [<DTC> ...]
    blurr transform [--debug] [--runner=<runner>] [--streaming-dtc=<dtc-file>] [--window-dtc=<dtc-file>] [--data-processor=<data-processor>] (--source=<raw-json-files> | <raw-json-files>)
    blurr package-spark [--debug] [--source-dir=<dir>] [--target=<zip-file>]
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
    package-spark   Generates a submittable Spark app with .zip extension. Requires a
                    requirements.txt inside --source-dir. The generated package
                    will contain all the python code inside the provided --source-dir,
                    along with all the dependencies resolved from requirements.txt.


Options:
    -h --help                   Show this screen.
    --version                   Show version.
    --runner=<runner>           The runner to use for the transform.
                                Possible values:
                                local - Transforms done in memory. <default>
                                spark - Transforms done using spark locally.
    --streaming-dtc=<dtc-file>  Streaming DTC file to use.
    --window-dtc=<dtc-file>     Window DTC file to use.
    --source=<raw-json-files>   List of source files separated by comma
    --debug                     Output debug logs.
    --data-processor=<data-processor>   Data processor to use to process each record.
                                        Possible values:
                                        simple - One event dictionary per line in the source file(s). <default>
                                        ipfix - Processor for IpFix format.
    --source-dir=<dir>          A directory containing a Spark app to be packaged. [default: ./]
    --target=<zip-file>         Filename of the generated Spark app zipfile. [default: spark-app.zip]
"""
import logging
import os
import sys

from docopt import docopt

from blurr.cli.cli import cli

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
VERSION_PATH = os.path.join(PACKAGE_DIR, 'VERSION')


def read_version(version_file: str) -> str:
    if os.path.exists(version_file) and os.path.isfile(version_file):
        with open(version_file, 'r') as file:
            version = file.readline()
            file.close()
        return version
    else:
        return 'LOCAL'


def setup_logs(debug: bool) -> None:
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    logger.addHandler(_handler)


def main():
    arguments = docopt(__doc__, version=read_version(VERSION_PATH))
    setup_logs(arguments['--debug'])
    result = cli(arguments)
    sys.exit(result)


if __name__ == '__main__':
    main()
