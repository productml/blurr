from typing import Dict, Any

import pytest
from pytest import fixture

from blurr.core.errors import MissingAttributeError, RequiredAttributeError
from blurr.core.schema_loader import SchemaLoader
from blurr.core.transformer import TransformerSchema, Transformer
from blurr.core.type import Type


@fixture
def schema_spec() -> Dict[str, Any]:
    return {
        'Name': 'test',
        'Type': Type.BLURR_TRANSFORM_STREAMING,
        'Version': '2018-03-01',
        'Aggregates': [{
            'Name': 'test_group',
            'Type': Type.BLURR_AGGREGATE_VARIABLE,
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
    name = schema_loader.add_schema_spec(schema_spec)
    return MockTransformer(MockTransformerSchema(name, schema_loader), 'user1')


def test_transformer_schema_init(schema_loader: SchemaLoader, schema_spec: Dict[str, Any]) -> None:
    name = schema_loader.add_schema_spec(schema_spec)
    test_transformer_schema = MockTransformerSchema(name, schema_loader)
    assert test_transformer_schema.version == '2018-03-01'
    assert test_transformer_schema.type == Type.BLURR_TRANSFORM_STREAMING


def test_transformer_init(test_transformer) -> None:
    assert test_transformer._identity == 'user1'
    assert test_transformer._evaluation_context.global_context == {
        'identity': 'user1',
        'test_group': test_transformer._aggregates['test_group']
    }
    assert test_transformer._evaluation_context.local_context == {}


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


def test_transformer_schema_missing_version_attribute_adds_error(schema_loader: SchemaLoader,
                                                                 schema_spec: Dict[str, Any]):
    del schema_spec[TransformerSchema.ATTRIBUTE_VERSION]
    del schema_spec[TransformerSchema.ATTRIBUTE_AGGREGATES]

    name = schema_loader.add_schema_spec(schema_spec)
    schema = MockTransformerSchema(name, schema_loader)

    assert 2 == len(schema.errors)
    assert isinstance(schema.errors[0], RequiredAttributeError)
    assert TransformerSchema.ATTRIBUTE_AGGREGATES == schema.errors[0].attribute
    assert isinstance(schema.errors[1], RequiredAttributeError)
    assert TransformerSchema.ATTRIBUTE_VERSION == schema.errors[1].attribute
