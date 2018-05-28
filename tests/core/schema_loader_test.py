from typing import Dict

from pytest import fixture

from blurr.core.errors import InvalidTypeError, RequiredAttributeError, SpecNotFoundError
from blurr.core.field_simple import IntegerFieldSchema
from blurr.core.schema_loader import SchemaLoader
from blurr.core.transformer_streaming import StreamingTransformerSchema
from blurr.core.type import Type
from blurr.core.validator import ATTRIBUTE_TYPE
from blurr.store.memory_store import MemoryStore


@fixture
def nested_schema_spec_bad_type() -> Dict:
    return {
        'Name': 'test',
        'Type': 'Blurr:Unknown',
        'Ignored': 2,
        'Aggregates': [{
            'Name': 'test_group',
            'Fields': [{
                "Name": "country",
                "Type": "string",
                "Value": "source.country"
            }, {
                "Name": "events",
                "Type": "integer",
                "Value": "test_group.events+1"
            }]
        }]
    }


@fixture
def nested_schema_spec() -> Dict:
    return {
        'Name': 'test',
        'Type': Type.BLURR_TRANSFORM_STREAMING,
        "Version": "2018-03-01",
        "Time": "parser.parse(source.event_time)",
        "Identity": "source.user_id",
        'Ignored': 2,
        'Aggregates': [{
            'Name': 'test_group',
            'Type': Type.BLURR_AGGREGATE_IDENTITY,
            'Fields': [{
                "Type": "string",
                "Name": "country",
                "Value": "source.country"
            }, {
                "Type": "integer",
                "Name": "events",
                "Value": "test_group.events+1"
            }]
        }]
    }


@fixture
def schema_loader(nested_schema_spec) -> SchemaLoader:
    schema_loader = SchemaLoader()
    schema_loader.add_schema_spec(nested_schema_spec)
    return schema_loader


def test_add_invalid_schema() -> None:
    schema_loader = SchemaLoader()

    assert schema_loader.add_schema_spec('') is None
    assert schema_loader.add_schema_spec(['test']) is None
    assert schema_loader.add_schema_spec({'test': 1}) is None


def test_add_valid_simple_schema() -> None:
    schema_loader = SchemaLoader()

    assert schema_loader.add_schema_spec({'Name': 'test', 'Type': 'test_type'}) == 'test'
    assert schema_loader.get_schema_spec('test') == {'Name': 'test', 'Type': 'test_type'}


def test_add_valid_simple_schema_with_parent() -> None:
    schema_loader = SchemaLoader()

    assert schema_loader.add_schema_spec({'Name': 'test', 'Type': 'test_type'}, 'parent') == 'test'
    assert schema_loader.get_schema_spec('parent.test') == {'Name': 'test', 'Type': 'test_type'}


def test_add_valid_nested_schema(nested_schema_spec_bad_type: Dict) -> None:
    schema_loader = SchemaLoader()

    assert schema_loader.add_schema_spec(nested_schema_spec_bad_type) == 'test'
    assert schema_loader.get_schema_spec('test.test_group') == nested_schema_spec_bad_type[
        'Aggregates'][0]
    assert schema_loader.get_schema_spec('test.test_group.country') == nested_schema_spec_bad_type[
        'Aggregates'][0]['Fields'][0]
    assert schema_loader.get_schema_spec('test.test_group.events') == nested_schema_spec_bad_type[
        'Aggregates'][0]['Fields'][1]


def test_get_schema_object_error(nested_schema_spec_bad_type: Dict) -> None:
    schema_loader = SchemaLoader()
    fqn = schema_loader.add_schema_spec(nested_schema_spec_bad_type)

    errors = schema_loader.get_errors()
    assert len(schema_loader.get_errors('test', True)) == 2

    assert InvalidTypeError('test', nested_schema_spec_bad_type, ATTRIBUTE_TYPE,
                            InvalidTypeError.Reason.TYPE_NOT_DEFINED) in errors
    assert RequiredAttributeError('test.test_group', nested_schema_spec_bad_type['Aggregates'],
                                  ATTRIBUTE_TYPE) in errors

    schema = schema_loader.get_schema_object('test')
    errors = schema_loader.get_errors()
    assert len(schema_loader.get_errors('test', True)) == 3
    assert schema is None
    assert InvalidTypeError(fqn, nested_schema_spec_bad_type, ATTRIBUTE_TYPE,
                            InvalidTypeError.Reason.TYPE_NOT_LOADED) in errors


def test_get_schema_object(schema_loader: SchemaLoader) -> None:
    assert isinstance(schema_loader.get_schema_object('test'), StreamingTransformerSchema) is True
    field_schema = schema_loader.get_schema_object('test.test_group.events')
    assert isinstance(field_schema, IntegerFieldSchema) is True

    # Assert that the same object is returned and a new one is not created.
    assert field_schema.when is None
    field_schema.when = 'True'
    assert schema_loader.get_schema_object('test.test_group.events').when == 'True'


def test_get_nested_schema_object(schema_loader: SchemaLoader):
    assert isinstance(
        schema_loader.get_nested_schema_object('test.test_group', 'events'),
        IntegerFieldSchema) is True


def test_get_fully_qualified_name() -> None:
    assert SchemaLoader.get_fully_qualified_name('parent', 'child') == 'parent.child'


def test_get_schemas_of_type(schema_loader: SchemaLoader, nested_schema_spec: Dict) -> None:
    assert schema_loader.get_schema_specs_of_type(Type.INTEGER) == {
        'test.test_group.events': nested_schema_spec['Aggregates'][0]['Fields'][1]
    }


def test_get_transformer_name() -> None:
    assert SchemaLoader.get_transformer_name('test.child1.child2') == 'test'


def test_get_store_error_not_declared(schema_loader: SchemaLoader):
    schema_loader.get_store('test.memstore')
    error = schema_loader.get_errors('test.memstore', False)[0]
    assert isinstance(error, SpecNotFoundError)


def test_get_store_error_wrong_type(schema_loader: SchemaLoader) -> None:
    schema_loader.get_store('test')
    error = next(
        x for x in schema_loader.get_errors('test', False) if isinstance(x, InvalidTypeError))
    assert isinstance(error, InvalidTypeError)
    assert error.reason == InvalidTypeError.Reason.INCORRECT_BASE
    assert error.expected_base_type == InvalidTypeError.BaseTypes.STORE


def test_get_store_success(nested_schema_spec: Dict) -> None:
    nested_schema_spec['Store'] = {'Name': 'memstore', 'Type': Type.BLURR_STORE_MEMORY}
    schema_loader = SchemaLoader()
    schema_loader.add_schema_spec(nested_schema_spec)
    assert isinstance(schema_loader.get_store('test.memstore'), MemoryStore)


def test_get_all_stores(nested_schema_spec: Dict) -> None:
    nested_schema_spec['Store'] = {'Name': 'memstore', 'Type': Type.BLURR_STORE_MEMORY}
    schema_loader = SchemaLoader()
    schema_loader.add_schema_spec(nested_schema_spec)

    # No store instantiated yet.
    assert schema_loader.get_all_stores() == []
    assert isinstance(schema_loader.get_store('test.memstore'), MemoryStore)

    stores = schema_loader.get_all_stores()
    assert len(stores) == 1
    assert isinstance(stores[0], MemoryStore)
