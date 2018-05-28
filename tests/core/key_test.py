from datetime import datetime

import pytest

from blurr.core.store_key import Key, KeyType


def test_invalid_identity():
    with pytest.raises(ValueError, match='`identity` must be present.'):
        Key(KeyType.DIMENSION, '', 'group')

    with pytest.raises(ValueError, match='`identity` must be present.'):
        Key(KeyType.DIMENSION, None, 'group')


def test_invalid_group():
    with pytest.raises(ValueError, match='`group` must be present.'):
        Key(KeyType.DIMENSION, 'id', '')

    with pytest.raises(ValueError, match='`group` must be present.'):
        Key(KeyType.DIMENSION, 'id', None)


def test_both_timestamp_and_dimension_error():
    with pytest.raises(
            ValueError, match='Both dimensions and timestamp should not be set together.'):
        Key(KeyType.DIMENSION, 'id', 'group', ['dim1'], datetime(2018, 3, 7, 22, 35, 31))


def test_key_type_and_args_error():
    with pytest.raises(ValueError, match='`timestamp` should not be set for KeyType.DIMENSION.'):
        Key(KeyType.DIMENSION, 'id', 'group', [], datetime(2018, 3, 7, 22, 35, 31))

    with pytest.raises(ValueError, match='`timestamp` should be set for KeyType.TIMESTAMP.'):
        Key(KeyType.TIMESTAMP, 'id', 'group', ['dim1'], None)


def test_parse():
    assert Key.parse('a/b//2018-03-07T22:35:31+00:00') == Key(KeyType.TIMESTAMP, 'a', 'b', [],
                                                              datetime(2018, 3, 7, 22, 35, 31))
    assert Key.parse('a/b/c:d/') == Key(KeyType.DIMENSION, 'a', 'b', ['c', 'd'])
    assert Key.parse('a/b//') == Key(KeyType.DIMENSION, 'a', 'b')
    with pytest.raises(Exception):
        Key.parse('')
    with pytest.raises(Exception):
        Key.parse('a//')
    with pytest.raises(Exception):
        Key.parse('a//b')
    with pytest.raises(Exception):
        Key.parse('None')


def test_equals_dimension_key():
    assert Key(KeyType.DIMENSION, 'a', 'b') == Key(KeyType.DIMENSION, 'a', 'b')
    assert Key(KeyType.DIMENSION, 'a', 'b') != Key(KeyType.DIMENSION, 'a', 'c')
    assert Key(KeyType.DIMENSION, 'a', 'b') != Key(KeyType.DIMENSION, 'a', 'b', ['c'])
    assert Key(KeyType.DIMENSION, 'a', 'b', ['c']) == Key(KeyType.DIMENSION, 'a', 'b', ['c'])
    assert Key(KeyType.DIMENSION, 'a', 'b', ['c']) != Key(KeyType.DIMENSION, 'a', 'b', ['d'])


def test_equals_timestamp_key():
    assert Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31)) == Key(
        KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31))
    assert Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31)) != Key(
        KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 6, 22, 35, 31))
    assert Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31)) != Key(
        KeyType.TIMESTAMP, 'a', 'c', [], datetime(2018, 3, 7, 22, 35, 31))


def test_greater_than_dimension_key():
    assert (Key(KeyType.DIMENSION, 'a', 'b') > Key(KeyType.DIMENSION, 'a', 'b')) is False
    assert (Key(KeyType.DIMENSION, 'a', 'b') > Key(KeyType.DIMENSION, 'a', 'c')) is False
    assert (Key(KeyType.DIMENSION, 'a', 'b') > Key(KeyType.DIMENSION, 'a', 'b', ['c'])) is False
    assert (Key(KeyType.DIMENSION, 'a', 'b', ['c']) > Key(KeyType.DIMENSION, 'a', 'b',
                                                          ['c'])) is False
    assert (Key(KeyType.DIMENSION, 'a', 'b', ['d']) > Key(KeyType.DIMENSION, 'a', 'b',
                                                          ['c'])) is True


def test_greater_than_timestamp_key():
    assert (Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31)) > Key(
        KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31))) is False
    assert (Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31)) > Key(
        KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 6, 22, 35, 31))) is True
    assert (Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31)) > Key(
        KeyType.TIMESTAMP, 'a', 'c', [], datetime(2018, 3, 7, 22, 35, 31))) is False


def test_less_than_dimension_key():
    assert (Key(KeyType.DIMENSION, 'a', 'b') < Key(KeyType.DIMENSION, 'a', 'b')) is False
    assert (Key(KeyType.DIMENSION, 'a', 'b') < Key(KeyType.DIMENSION, 'a', 'c')) is False
    assert (Key(KeyType.DIMENSION, 'a', 'b') < Key(KeyType.DIMENSION, 'a', 'b', ['c'])) is True
    assert (Key(KeyType.DIMENSION, 'a', 'b', ['c']) < Key(KeyType.DIMENSION, 'a', 'b',
                                                          ['c'])) is False
    assert (Key(KeyType.DIMENSION, 'a', 'b', ['c']) < Key(KeyType.DIMENSION, 'a', 'b',
                                                          ['d'])) is True


def test_less_than_timestamp_key():
    assert (Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31)) < Key(
        KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31))) is False
    assert (Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 6, 22, 35, 31)) < Key(
        KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31))) is True
    assert (Key(KeyType.TIMESTAMP, 'a', 'b', [], datetime(2018, 3, 7, 22, 35, 31)) < Key(
        KeyType.TIMESTAMP, 'a', 'c', [], datetime(2018, 3, 7, 22, 35, 31))) is False
