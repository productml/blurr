from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Tuple, Any, Dict, Iterable, Generator

import yaml

from blurr.core import logging
from blurr.core.aggregate_block import BlockAggregate
from blurr.core.errors import PrepareWindowMissingBlocksError
from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store import Store
from blurr.core.store_key import Key
from blurr.core.transformer_streaming import StreamingTransformer, StreamingTransformerSchema
from blurr.core.transformer_window import WindowTransformer
from blurr.core.type import Type
from blurr.runner.data_processor import DataProcessor


TimeAndRecord = Tuple[datetime, Record]


class Runner(ABC):
    """
    An abstract class that provides functionality to:
    - Convert raw events in Records
    - Process a list of Records for a user.

    A class that inherits from Runner should do the following:
    1. Call `get_per_identity_records()` using an iterator of the events available. This returns
        a generator which creates a Tuple[Identity, TimeAndRecord]] output.
    2. The Tuple[Identity, TimeAndRecord]] output should be grouped together by the
         Identity to create a List of TimeAndRecord per identity.
    3. Using the per identity list of TimeAndRecord `execute_per_identity_records()`
        should be called.
        - This returns Tuple[Identity, Tuple[Streaming DTC State, List of Window DTC output]].
        - `execute_per_identity_records()` can take in a existing old_state (old Streaming DTC
            State) so as to allow batch execution to make use of previous output.
    """

    def __init__(self, stream_dtc_file: str, window_dtc_file: Optional[str]):
        self._stream_dtc = yaml.safe_load(open(stream_dtc_file))
        self._window_dtc = None if window_dtc_file is None else yaml.safe_load(
            open(window_dtc_file))

        # TODO: Assume validation will be done separately.
        # This causes a problem when running the code on spark
        # as the validation yml file is inside the archived package
        # and yamale is not able to read that.
        # validate_schema_spec(self._stream_dtc)
        # if self._window_dtc is not None:
        #     validate_schema_spec(self._window_dtc)

    def execute_per_identity_records(
            self,
            identity: str,
            records: List[TimeAndRecord],
            old_state: Optional[Dict[Key, Any]] = None) -> Tuple[str, Tuple[Dict, List]]:
        """
        Executes the streaming and window DTC on the given records. An option old state can provided
        which initializes the state for execution. This is useful for batch execution where the
        previous state is written out to storage and can be loaded for the next batch run.

        :param identity: Identity of the records.
        :param records: List of TimeAndRecord to be processed.
        :param old_state: Streaming DTC state dictionary from a previous execution.
        :return: Tuple[Identity, Tuple[Identity, Tuple[Streaming DTC state dictionary,
            List of window DTC output]].
        """
        schema_loader = SchemaLoader()
        if records:
            records.sort(key=lambda x: x[0])
        else:
            records = []

        block_data = self._execute_stream_dtc(records, identity, schema_loader, old_state)
        window_data = self._execute_window_dtc(identity, schema_loader)

        return identity, (block_data, window_data)

    def get_per_identity_records(self, events: Iterable, data_processor: DataProcessor
                                 ) -> Generator[Tuple[str, TimeAndRecord], None, None]:
        """
        Uses the given iteratable events and the data processor convert the event into a list of
        Records along with its identity and time.
        :param events: iteratable events.
        :param data_processor: DataProcessor to process each event in events.
        :return: yields Tuple[Identity, TimeAndRecord] for all Records in events,
        """
        schema_loader = SchemaLoader()
        stream_dtc_name = schema_loader.add_schema_spec(self._stream_dtc)
        stream_transformer_schema: StreamingTransformerSchema = schema_loader.get_schema_object(
            stream_dtc_name)
        for event in events:
            try:
                for record in data_processor.process_data(event):
                    try:
                        id = stream_transformer_schema.get_identity(record)
                        time = stream_transformer_schema.get_time(record)
                        yield (id, (time, record))
                    except Exception as err:
                        logging.error('{} in parsing Record {}.'.format(err, record))
            except Exception as err:
                logging.error('{} in parsing Event {}.'.format(err, event))

    def _execute_stream_dtc(self,
                            identity_events: List[TimeAndRecord],
                            identity: str,
                            schema_loader: SchemaLoader,
                            old_state: Optional[Dict] = None) -> Dict[Key, Any]:
        if self._stream_dtc is None:
            return {}

        stream_dtc_name = schema_loader.add_schema_spec(self._stream_dtc)
        stream_transformer_schema = schema_loader.get_schema_object(stream_dtc_name)
        store = self._get_store(schema_loader)

        if old_state:
            for k, v in old_state.items():
                store.save(k, v)

        if identity_events:
            stream_transformer = StreamingTransformer(stream_transformer_schema, identity)

            for time, event in identity_events:
                stream_transformer.run_evaluate(event)
            stream_transformer.run_finalize()

        return self._get_store(schema_loader).get_all(identity)

    def _execute_window_dtc(self, identity: str, schema_loader: SchemaLoader) -> List[Dict]:
        if self._window_dtc is None:
            logging.debug('Window DTC not provided')
            return []

        stream_transformer = StreamingTransformer(
            self._get_streaming_transformer_schema(schema_loader), identity)
        all_data = self._get_store(schema_loader).get_all(identity)
        stream_transformer.run_restore(all_data)

        exec_context = Context()
        exec_context.add(stream_transformer._schema.name, stream_transformer)

        block_obj = None
        for aggregate in stream_transformer._nested_items.values():
            if not isinstance(aggregate, BlockAggregate):
                continue
            if block_obj is not None:
                raise Exception(('Window operation is supported against Streaming ',
                                 'DTC with only one BlockAggregate'))
            block_obj = aggregate

        if block_obj is None:
            raise Exception('No BlockAggregate found in the Streaming DTC file')

        window_data = []

        window_dtc_name = schema_loader.add_schema_spec(self._window_dtc)
        window_transformer_schema = schema_loader.get_schema_object(window_dtc_name)
        window_transformer = WindowTransformer(window_transformer_schema, identity, exec_context)

        logging.debug('Running Window DTC for identity {}'.format(identity))

        anchors = 0
        blocks = 0
        for key, data in all_data.items():
            if key.group != block_obj._schema.name:
                continue
            try:
                blocks += 1
                if window_transformer.run_evaluate(block_obj.run_restore(data)):
                    anchors += 1
                    window_data.append(window_transformer.run_flattened_snapshot)
            except PrepareWindowMissingBlocksError as err:
                logging.debug('{} with {}'.format(err, key))

        if anchors == 0:
            logging.debug('No anchors found for identity {} out of {} blocks'.format(
                identity, blocks))

        return window_data

    @staticmethod
    def _get_store(schema_loader: SchemaLoader) -> Store:
        stores = schema_loader.get_all_stores()
        if not stores:
            fq_name_and_schema = schema_loader.get_schema_specs_of_type(
                Type.BLURR_STORE_DYNAMO, Type.BLURR_STORE_MEMORY)
            return schema_loader.get_store(next(iter(fq_name_and_schema)))

        return stores[0]

    @staticmethod
    def _get_streaming_transformer_schema(
            schema_loader: SchemaLoader) -> StreamingTransformerSchema:
        fq_name_and_schema = schema_loader.get_schema_specs_of_type(Type.BLURR_TRANSFORM_STREAMING)
        return schema_loader.get_schema_object(next(iter(fq_name_and_schema)))

    @abstractmethod
    def execute(self, *args, **kwargs):
        NotImplemented('execute must be implemented')

    @abstractmethod
    def write_output_file(self, *args, **kwargs):
        NotImplemented('execute must be implemented')

    @abstractmethod
    def print_output(self, *args, **kwargs):
        NotImplemented('execute must be implemented')
