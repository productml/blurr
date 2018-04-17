from typing import List, Optional

from blurr.cli.util import get_stream_window_dtc_files, get_yml_files, eprint
from blurr.cli.validate import get_valid_yml_files
from blurr.runner.local_runner import LocalRunner
from blurr.runner.spark_runner import SparkRunner


RUNNER_CLASS = {
    'local': LocalRunner,
    'spark': SparkRunner,
}


def transform(runner: str, stream_dtc_file: Optional[str], window_dtc_file: Optional[str], data_processor: Optional[str],
              raw_json_files: List[str]) -> int:
    if stream_dtc_file is None and window_dtc_file is None:
        stream_dtc_file, window_dtc_file = get_stream_window_dtc_files(
            get_valid_yml_files(get_yml_files()))

    if stream_dtc_file is None:
        eprint('Streaming DTC file not provided and could not be found in '
               'the current directory.')
        return 1

    runner = RUNNER_CLASS.get(runner, LocalRunner)(raw_json_files, stream_dtc_file, window_dtc_file, data_processor)
    out = runner.execute()
    runner.print_output(out)

    return 0
