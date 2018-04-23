from datetime import datetime
from typing import Dict, Any

import pytest
from pytest import fixture

from blurr.core.errors import MissingAttributeError
from blurr.core.schema_loader import SchemaLoader
from blurr.core.store_key import Key
from blurr.core.transformer import TransformerSchema, Transformer


@fixture
def schema_spec() -> Dict[str, Any]:
    return {
        'Name': 'test',
        'Type': 'Blurr:Transform:Streaming',
        'Version': '2018-03-01',
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


class MockTransformerSchema(TransformerSchema):
    """
    This class is to test abstract behavior, and thus, adds no functionality
    """
    pass


class MockTransformer(Transformer):
    """
    This class is to test abstract behavior, and thus, adds no functionality
    """
    pass


@fixture
def schema_loader() -> SchemaLoader:
    return SchemaLoader()


@fixture
def test_transformer(schema_loader: SchemaLoader, schema_spec: Dict[str, Any]) -> MockTransformer:
    name = schema_loader.add_schema(schema_spec)
    return MockTransformer(MockTransformerSchema(name, schema_loader), 'user1')


def test_transformer_schema_init(schema_loader: SchemaLoader, schema_spec: Dict[str, Any]) -> None:
    name = schema_loader.add_schema(schema_spec)
    test_transformer_schema = MockTransformerSchema(name, schema_loader)
    assert test_transformer_schema.version == '2018-03-01'
    assert test_transformer_schema.type == 'Blurr:Transform:Streaming'
    assert test_transformer_schema.stores['memstore'].type == 'Blurr:Store:MemoryStore'


def test_transformer_init(test_transformer) -> None:
    assert test_transformer._identity == 'user1'
    assert test_transformer._evaluation_context.global_context == {
        'identity': 'user1',
        'test_group': test_transformer._aggregates['test_group']
    }
    assert test_transformer._evaluation_context.local_context == {}


def test_transformer_finalize(test_transformer: MockTransformer,
                              schema_loader: SchemaLoader) -> None:
    store = schema_loader.get_schema_object('test.memstore')

    test_transformer.finalize()
    assert store.get(Key('user1', 'test_group')) is None

    test_transformer._evaluation_context.global_add('time', datetime(2018, 3, 1, 10, 10, 10))
    test_transformer.evaluate()
    test_transformer.finalize()
    assert store.get(Key('user1', 'test_group')) == {
        '_identity': 'user1',
        'events': 1,
        '_end_time': datetime(2018, 3, 1, 10, 10, 10),
        '_start_time': datetime(2018, 3, 1, 10, 10, 10)
    }


def test_transformer_get_attr(test_transformer: MockTransformer) -> None:
    assert test_transformer.test_group == test_transformer._aggregates['test_group']


def test_transformer_get_attr_missing(test_transformer: MockTransformer) -> None:
    with pytest.raises(MissingAttributeError, match='missing_group not defined in test'):
        assert test_transformer.missing_group


def test_transformer_get_item(test_transformer: MockTransformer) -> None:
    assert test_transformer['test_group'] == test_transformer._aggregates['test_group']


def test_transformer_get_item_missing(test_transformer: MockTransformer) -> None:
    with pytest.raises(MissingAttributeError, match='missing_group not defined in test'):
        assert test_transformer['missing_group']
