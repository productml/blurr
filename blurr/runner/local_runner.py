"""
Usage:
    local_runner.py --raw-data=<files> --streaming-dtc=<file> [--window-dtc=<file>] [--output-file=<file>]
    local_runner.py (-h | --help)
"""
import csv
import json
from typing import List, Optional

import yaml
from collections import defaultdict
from dateutil import parser
from docopt import docopt

from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.syntax.schema_validator import validate
from blurr.runner.identity_runner import execute_dtc


class LocalRunner:
    def __init__(self,
                 local_json_files: List[str],
                 stream_dtc_file: str,
                 window_dtc_file: Optional[str] = None):
        self._raw_files = local_json_files
        self._schema_loader = SchemaLoader()

        self._stream_dtc = yaml.safe_load(open(stream_dtc_file))
        self._window_dtc = None if window_dtc_file is None else yaml.safe_load(
            open(window_dtc_file))
        self._validate_dtc_syntax()

        self._stream_dtc_name = self._schema_loader.add_schema(self._stream_dtc)
        self._stream_transformer_schema = self._schema_loader.get_schema_object(
            self._stream_dtc_name)

        self._user_events = defaultdict(list)
        self._block_data = {}
        self._window_data = {}

    def _validate_dtc_syntax(self) -> None:
        validate(self._stream_dtc)
        if self._window_dtc is not None:
            validate(self._window_dtc)

    def _consume_record(self, record: Record) -> None:
        source_context = Context({'source': record})
        source_context.add('parser', parser)
        identity = self._stream_transformer_schema.get_identity(source_context)
        time = self._stream_transformer_schema.get_time(source_context)

        self._user_events[identity].append((time, record))

    def _consume_file(self, file: str) -> None:
        with open(file) as f:
            for record in f:
                self._consume_record(Record(json.loads(record)))

    def execute_for_all_users(self) -> None:
        for identity, events in self._user_events.items():
            block_data, window_data = execute_dtc(events, identity, self._stream_dtc,
                                                  self._window_dtc)

            self._block_data.update(block_data)
            self._window_data[identity] = window_data

    def execute(self) -> None:
        for file in self._raw_files:
            self._consume_file(file)

        self.execute_for_all_users()

    def print_output(self) -> None:
        if self._window_dtc is not None:
            for row in self._window_data.items():
                print(json.dumps(row, default=str))
        else:
            for row in self._block_data.items():
                print(json.dumps(row, default=str))

    def write_output_file(self, output_file: str):
        header = []
        for data_rows in self._window_data.values():
            for data_row in data_rows:
                header = data_row.keys()
        with open(output_file, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, header)
            writer.writeheader()
            for data_rows in self._window_data.values():
                writer.writerows(data_rows)


def main():
    arguments = docopt(__doc__, version='pre-alpha')
    local_runner = LocalRunner(arguments['--raw-data'].split(','), arguments['--streaming-dtc'],
                               arguments['--window-dtc'])
    local_runner.execute()
    if arguments['--output-file'] is None:
        local_runner.print_output()
    else:
        local_runner.write_output_file(arguments['--output-file'])


if __name__ == "__main__":
    main()
