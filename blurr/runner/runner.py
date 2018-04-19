from datetime import datetime
from typing import List, Optional, Tuple, Any, Union

import yaml
from abc import ABC, abstractmethod

import blurr.runner.identity_runner as identity_runner
from blurr.core import logging
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.runner.data_processor import DataProcessor


class Runner(ABC):
    def __init__(self, json_files: List[str], stream_dtc_file: str, window_dtc_file: Optional[str],
                 data_processor: DataProcessor):
        self._raw_files = json_files
        self._schema_loader = SchemaLoader()
        self._data_processor = data_processor

        self._stream_dtc = yaml.safe_load(open(stream_dtc_file))
        self._window_dtc = None if window_dtc_file is None else yaml.safe_load(
            open(window_dtc_file))

        # TODO: Assume validation will be done separately.
        # This causes a problem when running the code on spark
        # as the validation yml file is inside the archived package
        # and yamale is not able to read that.
        # validate(self._stream_dtc)
        # if self._window_dtc is not None:
        #     validate(self._window_dtc)

        self._stream_dtc_name = self._schema_loader.add_schema(self._stream_dtc)
        self._stream_transformer_schema = self._schema_loader.get_schema_object(
            self._stream_dtc_name)

    def execute_per_identity_records(self,
                                     identity_records: Tuple[str, List[Tuple[datetime, Record]]]
                                     ) -> List[Union[Tuple[Key, Any], Tuple[str, Any]]]:
        identity, events = identity_records
        block_data, window_data = identity_runner.execute_dtc(events, identity, self._stream_dtc,
                                                              self._window_dtc)

        if self._window_dtc is None:
            return [(k, v) for k, v in block_data.items()]
        else:
            return [(identity, window_data)]

    def get_per_identity_records(self, event_str: str) -> List[Tuple[str, Tuple[datetime, Record]]]:
        record_list = []
        for record in self._data_processor.process_data(event_str):
            try:
                record_list.append((self._stream_transformer_schema.get_identity(record),
                                    (self._stream_transformer_schema.get_time(record), record)))
            except Exception as err:
                logging.debug('{} in parsing Record.'.format(err))
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
