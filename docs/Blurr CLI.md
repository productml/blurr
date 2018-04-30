# Installing the Command Line Interface (CLI)

One way to interact with Blurr is by using a Command Line Interface (CLI). The CLI is used to run blurr
locally and is a great way of validating and testing the DTCs before deploying them in 
production. 

Blurr is installed via pip:

`$ pip install blurr`

To check the installation was correct:

`$ blurr --version`

The help command lists all the commands:

`$ blurr --help`

## Usage
```
$ blurr --help
Usage:
    blurr validate [--debug] [<DTC> ...]
    blurr transform [--debug] [--runner=<runner>] [--streaming-dtc=<dtc-file>] [--window-dtc=<dtc-file>] \
            [--data-processor=<data-processor>] (--source=<raw-json-files> | <raw-json-files>)
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
    --runner=<runner>           The runner to use for the transform. Possible values:
                                local - Transforms done in memory. <default>
                                spark - Transforms done using spark locally.
    --streaming-dtc=<dtc-file>  Streaming DTC file to use.
    --window-dtc=<dtc-file>     Window DTC file to use.
    --source=<raw-json-files>   List of source files separated by comma
    --debug                             Output debug logs.
    --data-processor=<data-processor>   Data processor to use to process each record.
                                        Possible values:
                                        simple - One event dictionary per line in the source file(s). <default>
                                        ipfix - Processor for IpFix format.
```

Please create [an issue](https://github.com/productml/blurr/issues/new) to request for a new feature! Or better yet, contribute to Blurr and build it!

