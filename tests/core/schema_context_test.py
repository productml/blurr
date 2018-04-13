import pytest

from blurr.core.schema_context import SchemaContext


def test_import_empty():
    schema_context = SchemaContext([])
    context = schema_context.context

    assert not context.local_context
    assert not context.global_context


def test_import_just_module():
    spec = [{'Module': 'dateutil'}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    import dateutil
    assert context.global_context['dateutil'] == dateutil


def test_import_module_and_identifier():
    spec = [{'Module': 'dateutil', 'Identifier': ['parser']}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    from dateutil import parser
    assert context.global_context['parser'] == parser


def test_import_module_and_multiple_identifiers():
    spec = [{'Module': 'dateutil', 'Identifier': ['parser', 'tz']}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    from dateutil import parser
    from dateutil import tz
    assert context.global_context['parser'] == parser
    assert context.global_context['tz'] == tz


def test_import_just_module_with_alias():
    spec = [{'Module': 'dateutil as my_mod'}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    import dateutil
    assert context.global_context['my_mod'] == dateutil


def test_import_module_and_identifier_with_alias():
    spec = [{'Module': 'dateutil', 'Identifier': ['parser as my_func']}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    from dateutil import parser
    assert context.global_context['my_func'] == parser


def test_import_just_module_error():
    spec = [{'Module': 'unknown_module'}]
    schema_context = SchemaContext(spec)
    with pytest.raises(ModuleNotFoundError, match='No module named \'unknown_module\''):
        assert schema_context.context


def test_import_module_and_identifier_error():
    spec = [{'Module': 'dateutil', 'Identifier': ['unknown_func']}]
    schema_context = SchemaContext(spec)
    with pytest.raises(ImportError, match='cannot import name \'unknown_func\''):
        assert schema_context.context
