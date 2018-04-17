
import csv
from datetime import datetime
from typing import List, Optional, Tuple, Any, Dict, Union

import yaml
from abc import ABC, abstractmethod

import blurr.runner.identity_runner as identity_runner
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.syntax.schema_validator import validate
from blurr.runner.record_processor import SingleJsonDataProcessor, IpfixDataProcessor


DATA_PROCESSOR = {'ipfix': IpfixDataProcessor, 'default': SingleJsonDataProcessor}


class Runner(ABC):
    def __init__(self,
                 json_files: List[str],
                 stream_dtc_file: str,
                 window_dtc_file: Optional[str] = None,
                 data_processor: str = 'default'):
        self._raw_files = json_files
        self._schema_loader = SchemaLoader()
        self._record_processor = DATA_PROCESSOR.get(data_processor, SingleJsonDataProcessor)()

        self._stream_dtc = yaml.safe_load(open(stream_dtc_file))
        self._window_dtc = None if window_dtc_file is None else yaml.safe_load(
            open(window_dtc_file))
        self._validate_dtc_syntax()

        self._stream_dtc_name = self._schema_loader.add_schema(self._stream_dtc)
        self._stream_transformer_schema = self._schema_loader.get_schema_object(
            self._stream_dtc_name)

    def _validate_dtc_syntax(self) -> None:
        validate(self._stream_dtc)
        if self._window_dtc is not None:
            validate(self._window_dtc)

    def execute_per_user_events(self, user_events: Tuple[str, List[Tuple[datetime, Record]]]
                                ) -> Union[List[Tuple[Key, Any]], List[Dict]]:
        identity = user_events[0]
        events = user_events[1]
        block_data, window_data = identity_runner.execute_dtc(events, identity, self._stream_dtc,
                                                              self._window_dtc)

        print(block_data, window_data)
        if self._window_dtc is None:
            return [(k, v) for k, v in block_data.items()]
        else:
            return window_data

    def get_per_user_records(self, event_str: str) -> List[Tuple[str, Tuple[datetime, Record]]]:
        record_list = []
        for record in self._record_processor.process_data(event_str):
            record_list.append((self._stream_transformer_schema.get_identity(record),
                                (self._stream_transformer_schema.get_time(record), record)))
        return record_list

    @abstractmethod
    def execute(self, *args, **kwargs):
        NotImplemented('execute must be implemented')

    @abstractmethod
    def write_output_file(self, *args, **kwargs):
        NotImplemented('execute must be implemented')

    @abstractmethod
    def print_output(self, *args, **kwargs):
        NotImplemented('execute must be implemented')
