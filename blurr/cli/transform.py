from typing import List

from blurr.cli.out import Out
from blurr.cli.util import get_stream_window_dtc_files, get_yml_files
from blurr.cli.validate import get_valid_yml_files
from blurr.runner.local_runner import LocalRunner


def transform(stream_dtc_file: str, window_dtc_file: str,
              raw_json_files: List[str], out: Out) -> int:
    if stream_dtc_file is None and window_dtc_file is None:
        stream_dtc_file, window_dtc_file = get_stream_window_dtc_files(
            get_valid_yml_files(get_yml_files()))

    if stream_dtc_file is None and window_dtc_file is not None:
        out.eprint(
            'Streaming DTC needs to be provided if a Window DTC is provided.')
        return 1

    local_runner = LocalRunner(raw_json_files, stream_dtc_file,
                               window_dtc_file)
    local_runner.execute()
    local_runner.print_output()
