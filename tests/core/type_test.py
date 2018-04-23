from blurr.core.type import Type


def test_is_type_equal_returns_true_when_actual_type_is_equal_string():
    assert Type.is_type_equal("day", Type.DAY)


def test_is_type_equal_returns_true_when_actual_type_is_equal_type():
    assert Type.is_type_equal(Type.DAY, Type.DAY)


def test_is_type_equal_returns_false_when_actual_type_is_unequal_string():
    assert Type.is_type_equal("hour", Type.DAY) is False


def test_is_type_equal_returns_false_when_actual_type_is_unequal_type():
    assert Type.is_type_equal(Type.DAY, Type.HOUR) is False


def test_is_type_equal_returns_false_when_actual_type_is_invalid_string():
    assert Type.is_type_equal("invalid", Type.HOUR) is False
