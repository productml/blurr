"""
Usage:
    local_runner.py --raw-data=<files> --streaming-dtc=<file> [--window-dtc=<file>] [--output-file=<file>]
    local_runner.py (-h | --help)
"""
import csv
import json
from collections import defaultdict
from typing import List, Optional, Dict, Tuple

from blurr.core.syntax.schema_validator import validate
from blurr.runner.data_processor import DataProcessor, SimpleJsonDataProcessor
from blurr.runner.json_encoder import BlurrJSONEncoder
from blurr.runner.runner import Runner, TimeAndRecord


class LocalRunner(Runner):
    def __init__(self, stream_dtc_file: str, window_dtc_file: Optional[str] = None):
        super().__init__(stream_dtc_file, window_dtc_file)

        self._per_user_data = {}

    def _validate_dtc_syntax(self) -> None:
        validate(self._stream_dtc)
        if self._window_dtc is not None:
            validate(self._window_dtc)

    def _execute_for_all_identities(self,
                                    identity_records: Dict[str, List[TimeAndRecord]],
                                    old_state: Optional[Dict[str, Dict]] = None) -> None:
        if not old_state:
            old_state = {}
        for identity, records in identity_records.items():
            _, data = self.execute_per_identity_records(identity, records,
                                                        old_state.get(identity, None))
            self._per_user_data[identity] = data

        for identity, state in old_state.items():
            if identity not in self._per_user_data:
                self._per_user_data[identity] = (old_state[identity], [])

    def get_identity_records_from_json_files(
            self,
            local_json_files: List[str],
            data_processor: DataProcessor = SimpleJsonDataProcessor()
    ) -> Dict[str, List[TimeAndRecord]]:
        identity_records = defaultdict(list)
        for file in local_json_files:
            with open(file) as file_stream:
                for identity, record_with_datetime in self.get_per_identity_records(
                        file_stream, data_processor):
                    identity_records[identity].append(record_with_datetime)
        return identity_records

    def execute(self,
                identity_records: Dict[str, List[TimeAndRecord]],
                old_state: Optional[Dict[str, Dict]] = None) -> Dict[str, Tuple[Dict, List]]:
        self._execute_for_all_identities(identity_records, old_state)
        return self._per_user_data

    def print_output(self, per_user_data) -> None:
        for id, (block_data, window_data) in per_user_data.items():
            if not self._window_dtc:
                for data in block_data.items():
                    print(json.dumps(data, cls=BlurrJSONEncoder))
            else:
                print(json.dumps((id, window_data), cls=BlurrJSONEncoder))

    def write_output_file(self, output_file: str, per_user_data):
        if not self._window_dtc:
            with open(output_file, 'w') as file:
                for block_data, _ in per_user_data.values():
                    for row in block_data.items():
                        file.write(json.dumps(row, cls=BlurrJSONEncoder))
                        file.write('\n')
        else:
            header = []
            for _, window_data in per_user_data.values():
                for data_row in window_data:
                    header = list(data_row.keys())
                    break
            header.sort()
            with open(output_file, 'w') as csv_file:
                writer = csv.DictWriter(csv_file, header)
                writer.writeheader()
                for _, window_data in per_user_data.values():
                    writer.writerows(window_data)
