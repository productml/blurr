from typing import List, Dict, Tuple, Any, Optional

from datetime import datetime
from dateutil import parser

from blurr.core.evaluation import Context, EvaluationContext
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.session_data_group import SessionDataGroup, \
    SessionDataGroupSchema
from blurr.core.store import Key
from blurr.core.streaming_transformer import StreamingTransformerSchema, \
    StreamingTransformer
from blurr.core.window_transformer import WindowTransformerSchema, \
    WindowTransformer
from blurr.store.memory_store import MemoryStore


def execute_dtc(identity_events: List[Tuple[datetime, Record]], identity: str,
                stream_dtc_spec: Dict, window_dtc_spec: Dict
                ) -> Tuple[List[Tuple[Key, Any]], List[Dict]]:
    schema_loader = SchemaLoader()
    identity_events.sort(key=lambda x: x[0])

    session_data = execute_stream_dtc(identity_events, identity, schema_loader,
                                      stream_dtc_spec)
    window_data = execute_window_dtc(identity, schema_loader, window_dtc_spec)

    return session_data, window_data


def execute_stream_dtc(
        identity_events: List[Tuple[datetime, Record]], identity: str,
        schema_loader: SchemaLoader,
        stream_dtc_spec: Optional[Dict]) -> List[Tuple[Key, Any]]:
    if stream_dtc_spec is None:
        return []

    stream_dtc_name = schema_loader.add_schema(stream_dtc_spec)
    stream_transformer_schema = StreamingTransformerSchema(
        stream_dtc_name, schema_loader)
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

    session_aggregate_schemas = schema_loader.get_schemas_of_type(
        'ProductML:DTC:DataGroup:SessionAggregate')
    session_obj = SessionDataGroup(
        SessionDataGroupSchema(session_aggregate_schemas[0][0], schema_loader),
        identity, EvaluationContext())
    window_data = []
    all_data = dict(get_memory_store(schema_loader).get_all())

    window_dtc_name = schema_loader.add_schema(window_dtc_spec)
    window_transformer_schema = WindowTransformerSchema(
        window_dtc_name, schema_loader)
    window_transformer = WindowTransformer(window_transformer_schema, identity,
                                           exec_context)

    for key, data in all_data.items():
        if key.group != session_aggregate_schemas[0][1]['Name']:
            continue
        if window_transformer.evaluate_anchor(session_obj.restore(data)):
            window_data.append(window_transformer.flattened_snapshot)

    return window_data


def get_memory_store(schema_loader: SchemaLoader) -> MemoryStore:
    store_schemas = schema_loader.get_schemas_of_type(
        'ProductML:DTC:Store:MemoryStore')
    return schema_loader.get_schema_object(store_schemas[0][0])
