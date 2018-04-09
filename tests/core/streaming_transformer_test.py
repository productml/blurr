from typing import Dict, Any

import pytest
from pytest import fixture

from blurr.core.errors import IdentityError, TimeError
from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.streaming_transformer import StreamingTransformerSchema, StreamingTransformer


@fixture
def schema_spec() -> Dict[str, Any]:
    return {
        'Name': 'test',
        'Type': 'Blurr:Streaming',
        'Version': '2018-03-01',
        'Identity': '\'user1\'',
        'Time': 'datetime(2016,10,10)',
        'Stores': [{
            'Name': 'memstore',
            'Type': 'Blurr:Store:MemoryStore'
        }],
        'Aggregates': [{
            'Name': 'test_group',
            'Type': 'Blurr:Aggregate:IdentityAggregate',
            'Store': 'memstore',
            'Fields': [{
                "Type": "integer",
                "Name": "events",
                "Value": "test_group.events+1"
            }]
        }]
    }


@fixture
def schema_loader() -> SchemaLoader:
    return SchemaLoader()


def test_streaming_transformer_schema_schema_init(schema_loader: SchemaLoader,
                                                  schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    assert transformer_schema.identity.code_string == '\'user1\''
    assert transformer_schema.time.code_string == 'datetime(2016,10,10)'


def test_streaming_transformer_schema_get_identity_constant(schema_loader: SchemaLoader,
                                                            schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    assert transformer_schema.get_identity(Context()) == 'user1'


def test_streaming_transformer_schema_get_identity_from_record(schema_loader: SchemaLoader,
                                                               schema_spec: Dict[str, Any]) -> None:
    schema_spec['Identity'] = 'source.user'
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    assert transformer_schema.get_identity(Context({
        'source': Record({
            'user': 'user1'
        })
    })) == 'user1'


def test_streaming_transformer_schema_get_identity_error(schema_loader: SchemaLoader,
                                                         schema_spec: Dict[str, Any]) -> None:
    schema_spec['Identity'] = 'source.user'
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    with pytest.raises(IdentityError, match='Could not determine identity using source.user'):
        assert transformer_schema.get_identity(Context({'source': Record()}))


def test_streaming_transformer_schema_get_time_constant(schema_loader: SchemaLoader,
                                                        schema_spec: Dict[str, Any]) -> None:
    from datetime import datetime
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    assert transformer_schema.get_time(Context({'datetime': datetime})) == datetime(2016, 10, 10)


def test_streaming_transformer_schema_get_time_datetime_not_defined(
        schema_loader: SchemaLoader, schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    with pytest.raises(NameError, match='name \'datetime\' is not defined'):
        assert transformer_schema.get_time(Context())


def test_streaming_transformer_schema_get_time_type_error(schema_loader: SchemaLoader,
                                                          schema_spec: Dict[str, Any]) -> None:
    schema_spec['Time'] = '1'
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    with pytest.raises(TimeError, match='Could not determine time using 1'):
        assert transformer_schema.get_time(Context())


def test_streaming_transformer_evaluate_record_time_error(schema_loader: SchemaLoader,
                                                          schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    transformer = StreamingTransformer(transformer_schema, 'user1', Context())
    with pytest.raises(NameError, match='name \'datetime\' is not defined'):
        assert transformer.evaluate_record(Record())


def test_streaming_transformer_evaluate_record_user_mismatch(schema_loader: SchemaLoader,
                                                             schema_spec: Dict[str, Any]) -> None:
    from datetime import datetime
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    transformer = StreamingTransformer(transformer_schema, 'user2', Context({'datetime': datetime}))
    with pytest.raises(
            IdentityError,
            match='Identity in transformer \(user2\) and new record \(user1\) do not match'):
        assert transformer.evaluate_record(Record())


def test_streaming_transformer_evaluate_record(schema_loader: SchemaLoader,
                                               schema_spec: Dict[str, Any]) -> None:
    from datetime import datetime
    streaming_dtc = schema_loader.add_schema(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    transformer = StreamingTransformer(transformer_schema, 'user1', Context({'datetime': datetime}))
    transformer.evaluate_record(Record())

    assert transformer._snapshot == {'test_group': {'_identity': 'user1', 'events': 1}}
