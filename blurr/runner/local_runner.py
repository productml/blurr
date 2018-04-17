"""
Usage:
    local_runner.py --raw-data=<files> --streaming-dtc=<file> [--window-dtc=<file>] [--output-file=<file>]
    local_runner.py (-h | --help)
"""
import csv
import json
from typing import List, Optional, Any

from collections import defaultdict

from blurr.core.record import Record
from blurr.core.syntax.schema_validator import validate
from blurr.runner.record_processor import DataProcessor, SingleJsonDataProcessor
from blurr.runner.runner import Runner


class LocalRunner(Runner):
    def __init__(self,
                 local_json_files: List[str],
                 stream_dtc_file: str,
                 window_dtc_file: Optional[str] = None,
                 data_processor: DataProcessor = SingleJsonDataProcessor()):
        super().__init__(local_json_files, stream_dtc_file, window_dtc_file, data_processor)

        self._users_events = defaultdict(list)
        self._block_data = {}
        self._window_data = defaultdict(list)

    def _validate_dtc_syntax(self) -> None:
        validate(self._stream_dtc)
        if self._window_dtc is not None:
            validate(self._window_dtc)

    def _consume_record(self, record: Record) -> None:
        identity = self._stream_transformer_schema.get_identity(record)
        time = self._stream_transformer_schema.get_time(record)

        self._users_events[identity].append((time, record))

    def _consume_file(self, file: str) -> None:
        with open(file) as f:
            for data_str in f:
                for identity, time_record in self.get_per_user_records(data_str):
                    self._users_events[identity].append(time_record)

    def execute_for_all_users(self) -> None:
        for user_events in self._users_events.items():
            data = self.execute_per_user_events(user_events)
            if self._window_dtc:
                self._window_data[user_events[0]] = data
            else:
                self._block_data.update(data)

    def execute(self) -> Any:
        for file in self._raw_files:
            self._consume_file(file)

        self.execute_for_all_users()
        return self._window_data if self._window_dtc else self._block_data

    def print_output(self, data) -> None:
        for row in data.items():
            print(json.dumps(row, default=str))

    def write_output_file(self, output_file: str, data):
        header = []
        for data_rows in data.values():
            for data_row in data_rows:
                header = data_row.keys()
                break
        with open(output_file, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, header)
            writer.writeheader()
            for data_rows in self._window_data.values():
                writer.writerows(data_rows)
