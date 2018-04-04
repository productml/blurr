import pickle

import pytest

from blurr.core.record import Record


def test_simple_fields() -> None:
    record = Record({'string_field': 'string value', 'int_field': 1})

    assert record.string_field == 'string value'
    assert record.int_field == 1


def test_dict() -> None:
    record = Record({'dict_field': {'field_1': 'one', 'field_2': 2}})
    assert record.dict_field.field_1 == 'one'
    assert record.dict_field.field_2 == 2


def test_array() -> None:
    record = Record({'array_field': ['one', 'two', 'three']})
    assert len(record.array_field) == 3
    assert record.array_field[0] == 'one'


def test_complex_array() -> None:
    record = Record({'complex_array_field': [{'field_1': 'one', 'field_2': 2}, ['one', 'two'], 1]})
    assert len(record.complex_array_field) == 3
    assert record.complex_array_field[0].field_1 == 'one'
    assert record.complex_array_field[1][0] == 'one'
    assert record.complex_array_field[2] == 1


def test_invalid_field() -> None:
    record = Record({})
    assert record.missing_field is None


def test_pickle():
    record = Record({'test': 1})
    pickled_record = pickle.dumps(record)
    assert record == pickle.loads(pickled_record)


def test_getattr():
    record = Record({'test': 1})
    # Defined class attributes work as expected.
    assert record.__class__ == Record
    # Un-defined class attributes throw exception.
    with pytest.raises(AttributeError, match='Record object has no __test__ attribute.'):
        record.__test__
