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