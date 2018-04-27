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
    blurr validate [--debug] [&lt;DTC&gt; ...]
    blurr transform [--debug] [--runner=&lt;runner&gt;] [--streaming-dtc=&lt;dtc-file&gt;] [--window-dtc=&lt;dtc-file&gt;] [--data-processor=&lt;data-processor&gt;] (--source=&lt;raw-json-files&gt; | &lt;raw-json-files&gt;)
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
    --runner=&lt;runner&gt;     The runner to use for the transform. Possible values:
                                local - Transforms done in memory. &lt;default&gt;
                                spark - Transforms done using spark locally.
    --streaming-dtc=&lt;dtc-file&gt;  Streaming DTC file to use.
    --window-dtc=&lt;dtc-file&gt;       Window DTC file to use.
    --source=&lt;raw-json-files&gt;     List of source files separated by comma
    --debug                             Output debug logs.
    --data-processor=&lt;data-processor&gt;   Data processor to use to process each record.
                                        Possible values:
                                        simple - One event dictionary per line in the source file(s). &lt;default&gt;
                                        ipfix - Processor for IpFix format.
```

Please create [an issue](https://github.com/productml/blurr/issues/new) to request for a new feature! Or better yet, contribute to Blurr and build it!

