from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional

from dateutil import parser

from blurr.core.block_data_group import BlockDataGroup
from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.streaming_transformer import StreamingTransformer, \
    StreamingTransformerSchema
from blurr.core.window_transformer import WindowTransformer
from blurr.store.memory_store import MemoryStore


def execute_dtc(identity_events: List[Tuple[datetime, Record]], identity: str,
                stream_dtc_spec: Dict,
                window_dtc_spec: Dict) -> Tuple[Dict[Key, Any], List[Dict]]:
    schema_loader = SchemaLoader()
    identity_events.sort(key=lambda x: x[0])

    block_data = execute_stream_dtc(identity_events, identity, schema_loader,
                                    stream_dtc_spec)
    window_data = execute_window_dtc(identity, schema_loader, window_dtc_spec)

    return block_data, window_data


def execute_stream_dtc(identity_events: List[Tuple[datetime, Record]],
                       identity: str, schema_loader: SchemaLoader,
                       stream_dtc_spec: Optional[Dict]) -> Dict[Key, Any]:
    if stream_dtc_spec is None:
        return {}

    stream_dtc_name = schema_loader.add_schema(stream_dtc_spec)
    stream_transformer_schema = schema_loader.get_schema_object(
        stream_dtc_name)
    exec_context = Context()
    exec_context.add('parser', parser)

    stream_transformer = StreamingTransformer(stream_transformer_schema,
                                              identity, exec_context)
    for time, event in identity_events:
        stream_transformer.evaluate_record(event)
    stream_transformer.finalize()

    return get_memory_store(schema_loader).get_all()


def execute_window_dtc(identity: str, schema_loader: SchemaLoader,
                       window_dtc_spec: Optional[Dict]) -> List[Dict]:
    if window_dtc_spec is None:
        return []

    exec_context = Context()
    exec_context.add('parser', parser)

    stream_transformer = StreamingTransformer(
        get_streaming_transformer_schema(schema_loader), identity, Context())
    all_data = get_memory_store(schema_loader).get_all()
    stream_transformer.restore(all_data)

    exec_context.add(stream_transformer.schema.name, stream_transformer)

    block_obj = None
    for _, data_group in stream_transformer.nested_items.items():
        if isinstance(data_group, BlockDataGroup):
            block_obj = data_group
    if block_obj is None:
        return []

    window_data = []

    window_dtc_name = schema_loader.add_schema(window_dtc_spec)
    window_transformer_schema = schema_loader.get_schema_object(
        window_dtc_name)
    window_transformer = WindowTransformer(window_transformer_schema, identity,
                                           exec_context)

    for key, data in all_data.items():
        if key.group != block_obj.schema.name:
            continue
        if window_transformer.evaluate_anchor(block_obj.restore(data)):
            window_data.append(window_transformer.flattened_snapshot)

    return window_data


def get_memory_store(schema_loader: SchemaLoader) -> MemoryStore:
    store_schemas = schema_loader.get_schemas_of_type(
        'Blurr:Store:MemoryStore')
    return schema_loader.get_schema_object(store_schemas[0][0])


def get_streaming_transformer_schema(
        schema_loader: SchemaLoader) -> StreamingTransformerSchema:
    streaming_transformer_schema = schema_loader.get_schemas_of_type(
        'Blurr:Streaming')
    return schema_loader.get_schema_object(streaming_transformer_schema[0][0])
