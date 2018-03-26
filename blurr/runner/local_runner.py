import csv
import json
from typing import Dict, List, Optional

import yaml
from collections import defaultdict

from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.streaming_transformer import StreamingTransformerSchema
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
        self._stream_dtc_name = self._schema_loader.add_schema(
            self._stream_dtc)
        self._stream_transformer_schema = StreamingTransformerSchema(
            self._stream_dtc_name, self._schema_loader)

        self._window_dtc = None if window_dtc_file is None else yaml.safe_load(
            open(window_dtc_file))

        self._user_events = defaultdict(list)
        self._session_data = {}
        self._window_data = {}

        self._validate_dtc_syntax()

    def _validate_dtc_syntax(self) -> None:
        validate(self._stream_dtc)
        if self._window_dtc is not None:
            validate(self._window_dtc)

    def _consume_record(self, record: Dict) -> None:
        source_context = Context({'source': Record(record)})
        identity = self._stream_transformer_schema.get_identity(source_context)

        self._user_events[identity].append(record)

    def _consume_file(self, file: str) -> None:
        with open(file) as f:
            for record in f:
                self._consume_record(json.loads(record))

    def execute_for_all_users(self) -> None:
        for identity, events in self._user_events.items():
            session_data, window_data = execute_dtc(
                events, identity, self._stream_dtc, self._window_dtc)

            self._session_data.update(session_data)
            self._window_data[identity] = window_data

    def execute(self) -> None:
        for file in self._raw_files:
            self._consume_file(file)

        self.execute_for_all_users()

    def write_output_file(self, output_file: str):
        with open(output_file, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, self._window_data[0].keys())
            writer.writeheader()
            writer.writerows(self._window_data)


def main():
    pass


if __name__ == "__main__":
    main()
