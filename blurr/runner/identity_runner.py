from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional

from dateutil import parser

from blurr.core import logging
from blurr.core.aggregate_block import BlockAggregate
from blurr.core.errors import PrepareWindowMissingBlocksError
from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.transformer_streaming import StreamingTransformer, \
    StreamingTransformerSchema
from blurr.core.transformer_window import WindowTransformer
from blurr.store.memory_store import MemoryStore


def execute_dtc(identity_events: List[Tuple[datetime, Record]], identity: str,
                stream_dtc_spec: Dict, window_dtc_spec: Dict) -> Tuple[Dict[Key, Any], List[Dict]]:
    schema_loader = SchemaLoader()
    identity_events.sort(key=lambda x: x[0])

    block_data = execute_stream_dtc(identity_events, identity, schema_loader, stream_dtc_spec)
    window_data = execute_window_dtc(identity, schema_loader, window_dtc_spec)

    return block_data, window_data


def execute_stream_dtc(identity_events: List[Tuple[datetime, Record]], identity: str,
                       schema_loader: SchemaLoader,
                       stream_dtc_spec: Optional[Dict]) -> Dict[Key, Any]:
    if stream_dtc_spec is None:
        return {}

    stream_dtc_name = schema_loader.add_schema(stream_dtc_spec)
    stream_transformer_schema = schema_loader.get_schema_object(stream_dtc_name)
    exec_context = Context()
    exec_context.add('parser', parser)

    stream_transformer = StreamingTransformer(stream_transformer_schema, identity, exec_context)
    for time, event in identity_events:
        stream_transformer.evaluate_record(event)
    stream_transformer.finalize()

    return get_memory_store(schema_loader).get_all()


def execute_window_dtc(identity: str, schema_loader: SchemaLoader,
                       window_dtc_spec: Optional[Dict]) -> List[Dict]:
    if window_dtc_spec is None:
        logging.debug('Window DTC not provided')
        return []

    exec_context = Context()
    exec_context.add('parser', parser)

    stream_transformer = StreamingTransformer(
        get_streaming_transformer_schema(schema_loader), identity, Context())
    all_data = get_memory_store(schema_loader).get_all()
    stream_transformer.restore(all_data)

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

    window_dtc_name = schema_loader.add_schema(window_dtc_spec)
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
            if window_transformer.evaluate_anchor(block_obj.restore(data)):
                anchors += 1
                window_data.append(window_transformer.flattened_snapshot)
        except PrepareWindowMissingBlocksError as err:
            logging.debug('{} with {}'.format(err, key))

    if anchors == 0:
        logging.debug('No anchors found for identity {} out of {} blocks'.format(identity, blocks))

    return window_data


def get_memory_store(schema_loader: SchemaLoader) -> MemoryStore:
    store_schemas = schema_loader.get_schemas_of_type('Blurr:Store:MemoryStore')
    return schema_loader.get_schema_object(store_schemas[0][0])


def get_streaming_transformer_schema(schema_loader: SchemaLoader) -> StreamingTransformerSchema:
    streaming_transformer_schema = schema_loader.get_schemas_of_type('Blurr:Transform:Streaming')
    return schema_loader.get_schema_object(streaming_transformer_schema[0][0])
