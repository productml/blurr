import pytest

from blurr.core.schema_context import SchemaContext


def test_import_empty():
    schema_context = SchemaContext([])
    context = schema_context.context

    assert not context.local_context
    assert not context.global_context


def test_import_just_module():
    spec = [{'Module': 'datetime'}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    import datetime
    assert context.global_context['datetime'] == datetime


def test_import_module_and_identifier():
    spec = [{'Module': 'datetime', 'Identifiers': ['datetime']}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    from datetime import datetime
    assert context.global_context['datetime'] == datetime


def test_import_module_and_multiple_identifiers():
    spec = [{'Module': 'datetime', 'Identifiers': ['datetime', 'timezone']}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    from datetime import datetime
    from datetime import timezone
    assert context.global_context['datetime'] == datetime
    assert context.global_context['timezone'] == timezone


def test_import_just_module_with_alias():
    spec = [{'Module': 'datetime as my_mod'}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    import datetime
    assert context.global_context['my_mod'] == datetime


def test_import_module_and_identifier_with_alias():
    spec = [{'Module': 'datetime', 'Identifiers': ['datetime as my_func']}]
    schema_context = SchemaContext(spec)
    context = schema_context.context

    assert not context.local_context
    assert context.global_context

    from datetime import datetime
    assert context.global_context['my_func'] == datetime


def test_import_just_module_error():
    spec = [{'Module': 'unknown_module'}]
    schema_context = SchemaContext(spec)
    with pytest.raises(ModuleNotFoundError, match='No module named \'unknown_module\''):
        assert schema_context.context


def test_import_module_and_identifier_error():
    spec = [{'Module': 'datetime', 'Identifiers': ['unknown_func']}]
    schema_context = SchemaContext(spec)
    with pytest.raises(ImportError, match='cannot import name \'unknown_func\''):
        assert schema_context.context
