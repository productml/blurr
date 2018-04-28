from typing import List, Optional

from blurr.cli.util import get_stream_window_dtc_files, get_yml_files, eprint
from blurr.cli.validate import get_valid_yml_files
from blurr.runner.data_processor import IpfixDataProcessor, SimpleJsonDataProcessor, DataProcessor
from blurr.runner.local_runner import LocalRunner
from blurr.runner.spark_runner import SparkRunner

RUNNER_CLASS = {
    'local': LocalRunner,
    'spark': SparkRunner,
}

DATA_PROCESSOR_CLASS = {'ipfix': IpfixDataProcessor, 'simple': SimpleJsonDataProcessor}


def transform(runner: Optional[str], stream_dtc_file: Optional[str], window_dtc_file: Optional[str],
              data_processor: Optional[str], raw_json_files: List[str]) -> int:
    if stream_dtc_file is None and window_dtc_file is None:
        stream_dtc_file, window_dtc_file = get_stream_window_dtc_files(
            get_valid_yml_files(get_yml_files()))

    if stream_dtc_file is None:
        eprint('Streaming DTC file not provided and could not be found in '
               'the current directory.')
        return 1

    if not runner:
        runner = 'local'

    if not data_processor:
        data_processor = 'simple'

    if runner not in RUNNER_CLASS:
        eprint('Unknown runner: \'{}\'. Possible values: {}'.format(runner, list(
            RUNNER_CLASS.keys())))
        return 1

    if data_processor not in DATA_PROCESSOR_CLASS:
        eprint('Unknown data-processor: \'{}\'. Possible values: {}'.format(
            runner, list(DATA_PROCESSOR_CLASS.keys())))
        return 1

    data_processor_obj = DATA_PROCESSOR_CLASS[data_processor]()
    if runner == 'local':
        return transform_local(stream_dtc_file, window_dtc_file, raw_json_files, data_processor_obj)
    else:
        return transform_spark(stream_dtc_file, window_dtc_file, raw_json_files, data_processor_obj)


def transform_spark(stream_dtc_file: Optional[str], window_dtc_file: Optional[str],
                    raw_json_files: List[str], data_processor: DataProcessor) -> int:
    runner = SparkRunner(stream_dtc_file, window_dtc_file)
    out = runner.execute(runner.get_record_rdd_from_json_files(raw_json_files, data_processor))
    runner.print_output(out)

    return 0


def transform_local(stream_dtc_file: Optional[str], window_dtc_file: Optional[str],
                    raw_json_files: List[str], data_processor: DataProcessor) -> int:
    runner = LocalRunner(stream_dtc_file, window_dtc_file)
    out = runner.execute(
        runner.get_identity_records_from_json_files(raw_json_files, data_processor))
    runner.print_output(out)

    return 0
