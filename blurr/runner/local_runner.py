"""
Usage:
    local_runner.py --raw-data=<files> --streaming-dtc=<file> [--window-dtc=<file>] [--output-file=<file>]
    local_runner.py (-h | --help)
"""
import csv
import json
from collections import defaultdict
from datetime import datetime
from typing import List, Optional, Any, Dict, Tuple

from blurr.core.record import Record
from blurr.core.syntax.schema_validator import validate
from blurr.runner.data_processor import DataProcessor, SimpleJsonDataProcessor
from blurr.runner.runner import Runner, BlurrJSONEncoder


class LocalRunner(Runner):
    def __init__(self, stream_dtc_file: str, window_dtc_file: Optional[str] = None):
        super().__init__(stream_dtc_file, window_dtc_file)

        self._block_data = {}
        self._window_data = defaultdict(list)

    def _validate_dtc_syntax(self) -> None:
        validate(self._stream_dtc)
        if self._window_dtc is not None:
            validate(self._window_dtc)

    def _execute_for_all_identities(
            self, identity_records: Dict[str, List[Tuple[datetime, Record]]]) -> None:
        for per_identity_records in identity_records.items():
            data = self.execute_per_identity_records(per_identity_records)
            if self._window_dtc:
                self._window_data.update(data)
            else:
                self._block_data.update(data)

    def get_identity_records_from_json_files(
            self,
            local_json_files: List[str],
            data_processor: DataProcessor = SimpleJsonDataProcessor()
    ) -> Dict[str, List[Tuple[datetime, Record]]]:
        identity_records = defaultdict(list)
        for file in local_json_files:
            with open(file) as file_stream:
                for identity, record_with_datetime in self.get_per_identity_records(
                        file_stream, data_processor):
                    identity_records[identity].append(record_with_datetime)
        return identity_records

    def execute(self, identity_records: Dict[str, List[Tuple[datetime, Record]]]) -> Any:
        self._execute_for_all_identities(identity_records)
        return self._window_data if self._window_dtc else self._block_data

    def print_output(self, data) -> None:
        for row in data.items():
            print(json.dumps(row, cls=BlurrJSONEncoder))

    def write_output_file(self, output_file: str, data):
        if not self._window_dtc:
            with open(output_file, 'w') as file:
                for row in data.items():
                    file.write(json.dumps(row, cls=BlurrJSONEncoder))
                    file.write('\n')
        else:
            header = []
            for data_rows in data.values():
                for data_row in data_rows:
                    header = list(data_row.keys())
                    break
            header.sort()
            with open(output_file, 'w') as csv_file:
                writer = csv.DictWriter(csv_file, header)
                writer.writeheader()
                for data_rows in data.values():
                    writer.writerows(data_rows)
