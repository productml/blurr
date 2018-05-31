# Installing the Command Line Interface (CLI)

One way to interact with Blurr is by using a Command Line Interface (CLI). The CLI is used to run blurr
locally and is a great way of validating and testing the BTSs before deploying them in 
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
    blurr validate [--debug] [<BTS> ...]
    blurr transform [--debug] [--runner=<runner>] [--streaming-bts=<bts-file>] [--window-bts=<bts-file>] \
            [--data-processor=<data-processor>] (--source=<raw-json-files> | <raw-json-files>)
    blurr -h | --help

Commands:
    validate        Runs syntax validation on the list of BTS files provided. If
                    no files are provided then all *.bts files in the current
                    directory are validated.
    transform       Runs blurr to process the given raw log file. This command
                    can be run with the following combinations:
                    1. No BTS provided - The current directory is searched for
                    BTSs. First streaming and the first window BTS are used.
                    2. Only streaming BTS given - Transform outputs the result of
                    applying the BTS on the raw data file.
                    3. Both streaming and window BTS are provided - Transform
                    outputs the final result of applying the streaming and window
                    BTS on the raw data file.

Options:
    -h --help                   Show this screen.
    --version                   Show version.
    --runner=<runner>           The runner to use for the transform. Possible values:
                                local - Transforms done in memory. <default>
                                spark - Transforms done using spark locally.
    --streaming-bts=<bts-file>  Streaming BTS file to use.
    --window-bts=<bts-file>     Window BTS file to use.
    --source=<raw-json-files>   List of source files separated by comma
    --debug                             Output debug logs.
    --data-processor=<data-processor>   Data processor to use to process each record.
                                        Possible values:
                                        simple - One event dictionary per line in the source file(s). <default>
                                        ipfix - Processor for IpFix format.
```

Please create [an issue](https://github.com/productml/blurr/issues/new) to request for a new feature! Or better yet, contribute to Blurr and build it!

