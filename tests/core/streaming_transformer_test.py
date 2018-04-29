from datetime import datetime
from typing import Dict, Any

import pytest
from pytest import fixture

from blurr.core.aggregate import AggregateSchema
from blurr.core.errors import IdentityError, TimeError, RequiredAttributeError
from blurr.core.evaluation import Context
from blurr.core.record import Record
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.transformer_streaming import StreamingTransformerSchema, StreamingTransformer
from blurr.core.type import Type


@fixture
def schema_spec() -> Dict[str, Any]:
    return {
        'Name': 'test',
        'Type': Type.BLURR_TRANSFORM_STREAMING,
        'Version': '2018-03-01',
        'Import': [{
            'Module': 'datetime',
            'Identifiers': ['datetime']
        }],
        'Identity': '\'user1\'',
        'Time': 'datetime(2016,10,10)',
        'Stores': [{
            'Name': 'memstore',
            'Type': Type.BLURR_STORE_MEMORY
        }],
        'Aggregates': [{
            'Name': 'test_group',
            'Type': Type.BLURR_AGGREGATE_IDENTITY,
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
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    assert transformer_schema.identity.code_string == '\'user1\''
    assert transformer_schema.time.code_string == 'datetime(2016,10,10)'


def test_streaming_transformer_schema_get_identity_constant(schema_loader: SchemaLoader,
                                                            schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    assert transformer_schema.get_identity(Context()) == 'user1'


def test_streaming_transformer_schema_get_identity_from_record(schema_loader: SchemaLoader,
                                                               schema_spec: Dict[str, Any]) -> None:
    schema_spec['Identity'] = 'source.user'
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    assert transformer_schema.get_identity(Record({'user': 'user1'})) == 'user1'


def test_streaming_transformer_schema_get_identity_error(schema_loader: SchemaLoader,
                                                         schema_spec: Dict[str, Any]) -> None:
    schema_spec['Identity'] = 'source.user'
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    with pytest.raises(IdentityError, match='Could not determine identity using source.user'):
        assert transformer_schema.get_identity(Record())


def test_streaming_transformer_schema_get_time_constant(schema_loader: SchemaLoader,
                                                        schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    assert transformer_schema.get_time(Record()) == datetime(2016, 10, 10)


def test_streaming_transformer_schema_get_time_datetime_not_defined(
        schema_loader: SchemaLoader, schema_spec: Dict[str, Any]) -> None:
    del schema_spec['Import']
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    with pytest.raises(NameError, match='name \'datetime\' is not defined'):
        assert transformer_schema.get_time(Context())


def test_streaming_transformer_schema_get_time_type_error(schema_loader: SchemaLoader,
                                                          schema_spec: Dict[str, Any]) -> None:
    schema_spec['Time'] = '1'
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    with pytest.raises(TimeError, match='Could not determine time using 1'):
        assert transformer_schema.get_time(Context())


def test_streaming_transformer_evaluate_time_error(schema_loader: SchemaLoader,
                                                   schema_spec: Dict[str, Any]) -> None:
    del schema_spec['Import']
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    transformer = StreamingTransformer(transformer_schema, 'user1')
    with pytest.raises(NameError, match='name \'datetime\' is not defined'):
        assert transformer.evaluate(Record())


def test_streaming_transformer_evaluate_user_mismatch(schema_loader: SchemaLoader,
                                                      schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    transformer = StreamingTransformer(transformer_schema, 'user2')
    with pytest.raises(
            IdentityError,
            match='Identity in transformer \(user2\) and new record \(user1\) do not match'):
        assert transformer.evaluate(Record())


def test_streaming_transformer_evaluate(schema_loader: SchemaLoader,
                                        schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    transformer = StreamingTransformer(transformer_schema, 'user1')
    transformer.evaluate(Record())

    assert transformer._snapshot == {'test_group': {'_identity': 'user1', 'events': 1}}


def test_streaming_transformer_finalize(schema_loader: SchemaLoader,
                                        schema_spec: Dict[str, Any]) -> None:
    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    transformer_schema = StreamingTransformerSchema(streaming_dtc, schema_loader)
    transformer = StreamingTransformer(transformer_schema, 'user1')
    store = schema_loader.get_schema_object('test.memstore')

    transformer.finalize()
    assert store.get(Key('user1', 'test_group')) is None

    transformer.evaluate(Record())
    transformer.finalize()
    assert store.get(Key('user1', 'test_group')) == {'_identity': 'user1', 'events': 1}


def test_streaming_transformer_schema_missing_attributes_adds_error(schema_loader: SchemaLoader,
                                                                    schema_spec: Dict[str, Any]):
    del schema_spec[StreamingTransformerSchema.ATTRIBUTE_IDENTITY]
    del schema_spec[StreamingTransformerSchema.ATTRIBUTE_TIME]
    del schema_spec[StreamingTransformerSchema.ATTRIBUTE_STORES]
    del schema_spec[StreamingTransformerSchema.ATTRIBUTE_AGGREGATES][0][AggregateSchema.ATTRIBUTE_STORE]
    schema_spec[StreamingTransformerSchema.ATTRIBUTE_AGGREGATES][0][
        AggregateSchema.ATTRIBUTE_TYPE] = Type.BLURR_AGGREGATE_VARIABLE

    streaming_dtc = schema_loader.add_schema_spec(schema_spec)
    schema = StreamingTransformerSchema(streaming_dtc, schema_loader)

    assert 3 == len(schema.errors)
    assert isinstance(schema.errors[0], RequiredAttributeError)
    assert StreamingTransformerSchema.ATTRIBUTE_IDENTITY == schema.errors[0].attribute
    assert isinstance(schema.errors[1], RequiredAttributeError)
    assert StreamingTransformerSchema.ATTRIBUTE_TIME == schema.errors[1].attribute
    assert isinstance(schema.errors[2], RequiredAttributeError)
    assert StreamingTransformerSchema.ATTRIBUTE_STORES == schema.errors[2].attribute
