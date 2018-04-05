from datetime import datetime

import pytest

from blurr.core.store_key import Key


def test_invalid_identity():
    with pytest.raises(ValueError, match='`identity` must be present.'):
        Key('', 'group')

    with pytest.raises(ValueError, match='`identity` must be present.'):
        Key(None, 'group')


def test_invalid_group():
    with pytest.raises(ValueError, match='`group` must be present.'):
        Key('id', '')

    with pytest.raises(ValueError, match='`group` must be present.'):
        Key('id', None)


def test_parse():
    assert Key.parse('a/b/2018-03-07T22:35:31+00:00') == Key('a', 'b',
                                                             datetime(2018, 3, 7, 22, 35, 31))
    assert Key.parse('a/b') == Key('a', 'b', None)
    with pytest.raises(Exception):
        Key.parse('')
    with pytest.raises(Exception):
        Key.parse('a//')
    with pytest.raises(Exception):
        Key.parse('a//b')
    with pytest.raises(Exception):
        Key.parse('None')


def test_equals():
    assert Key('a', 'b') == Key('a', 'b')
    assert Key('a', 'b') != Key('a', 'c')
    assert Key('a', 'b') != Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31))
    assert Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) == Key('a', 'b',
                                                                  datetime(2018, 3, 7, 22, 35, 31))
    assert Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) != Key('a', 'b',
                                                                  datetime(2018, 3, 6, 22, 35, 31))


def test_greater_than():
    assert (Key('a', 'b') > Key('a', 'b')) is False
    assert (Key('a', 'b') > Key('a', 'c')) is False
    assert (Key('a', 'b') > Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31))) is True
    assert (Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) > Key('a', 'b')) is False
    assert (Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) > Key(
        'a', 'b', datetime(2018, 3, 7, 22, 35, 31))) is False
    assert (Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) > Key(
        'a', 'b', datetime(2018, 3, 8, 22, 35, 31))) is False
    assert (Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) > Key(
        'a', 'b', datetime(2018, 3, 6, 22, 35, 31))) is True


def test_less_than():
    assert (Key('a', 'b') < Key('a', 'b')) is False
    assert (Key('a', 'b') < Key('a', 'c')) is False
    assert (Key('a', 'b') < Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31))) is False
    assert (Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) < Key('a', 'b')) is True
    assert (Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) < Key(
        'a', 'b', datetime(2018, 3, 7, 22, 35, 31))) is False
    assert (Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) < Key(
        'a', 'b', datetime(2018, 3, 6, 22, 35, 31))) is False
    assert (Key('a', 'b', datetime(2018, 3, 7, 22, 35, 31)) < Key(
        'a', 'b', datetime(2018, 3, 8, 22, 35, 31))) is True
