from pytest import fixture, raises

from blurr.core.errors import SchemaErrorCollection, RequiredAttributeError, EmptyAttributeError, \
    InvalidIdentifierError, SchemaError, SchemaErrorCollectionFormatter

fully_qualified_name = 'fqn'


@fixture
def spec():
    return {'empty': '', 'identity': '_some thing'}


@fixture
def errors(spec):
    return [
        RequiredAttributeError(fully_qualified_name, spec, 'missing'),
        EmptyAttributeError(fully_qualified_name, spec, 'empty'),
        InvalidIdentifierError(fully_qualified_name, spec, 'identity',
                               InvalidIdentifierError.Reason.STARTS_WITH_UNDERSCORE),
    ]


def test_schema_error_collection_add_valid_and_invalid_items(errors):
    collection = SchemaErrorCollection()
    collection.add(errors[0])
    collection.add(Exception())
    collection.add(1)

    assert len(collection.errors) == 1
    assert collection[fully_qualified_name][0] == errors[0]


def test_schema_error_collection_add_list_of_errors(errors):
    collection = SchemaErrorCollection()
    collection.add(errors)

    assert len(collection.errors) == 3
    assert len(collection[fully_qualified_name]) == 3
    assert errors[0] in collection[fully_qualified_name]
    assert errors[1] in collection[fully_qualified_name]
    assert errors[2] in collection[fully_qualified_name]


def test_schema_error_collection_merge(spec, errors):
    error = RequiredAttributeError(fully_qualified_name, spec, 'k')
    collection = SchemaErrorCollection(error)
    collection2 = SchemaErrorCollection(errors)

    assert len(collection.errors) == 1

    collection.add(collection2.errors)

    assert len(collection[fully_qualified_name]) == 4
    assert error in collection[fully_qualified_name]
    assert errors[0] in collection[fully_qualified_name]
    assert errors[1] in collection[fully_qualified_name]
    assert errors[2] in collection[fully_qualified_name]


def test_schema_error_collection_raise(errors):
    collection = SchemaErrorCollection(errors)

    with raises(SchemaError) as err:
        collection.raise_errors()

    assert err.value.errors == collection


def test_schema_error_collection_formatter(errors):
    collection = SchemaErrorCollection(errors)
    formatter = SchemaErrorCollectionFormatter(line_separator='\n')

    err = formatter.format(collection)
    assert err.startswith('''
fqn
===''')
    assert '--> Attribute `missing` must be present under `fqn`.\n' in err
    assert '--> Attribute `empty` under `fqn` cannot be left empty.\n' in err
    assert '--> `identity: _some thing` in section `fqn` is invalid. Identifiers starting with underscore `_` are reserved.' in err
