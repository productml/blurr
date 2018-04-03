from blurr.core.field import ComplexTypeBase


class BaseType:
    """ Base Type which stores values and can return itself """

    def __init__(self, itself=None):
        if itself and hasattr(itself, 'base_field'):
            self.base_field = itself.base_field
        else:
            self.base_field = True

    @property
    def self_property(self):
        return self

    def base_method_without_return(self):
        pass


class TestType(ComplexTypeBase, BaseType):
    """
    Test type that extends Complex Type Base and some base type
    """

    def __init__(self, itself=None):
        if itself and hasattr(itself, 'test_field'):
            self.test_field = itself.test_field
        else:
            self.test_field = 2

        super().__init__(itself)

    def method_with_return(self, arg1, arg2):
        return self.test_field, arg1, arg2

    def method_without_return(self):
        pass

    def __str__(self):
        return 'string'


def test_field_access():
    """ Ensures that fields can be accessed transparently """
    sample = TestType()

    assert sample.test_field == 2
    assert sample.base_field is True


def test_property_access_and_return_type_overwriting():
    """ Ensures properties are executed properly, and base object returns are type-casted to the sub class"""
    sample = TestType()
    sample.test_field = 1
    assert isinstance(sample.self_property, TestType)
    assert sample.self_property.test_field == 1


def test_method_with_no_return():
    """ Tests that when methods return no value, the current object is returned """
    sample = TestType()
    assert sample.method_without_return() is sample


def test_method_with_return():
    """ Tests that when methods do return some value, they are returned as-is """
    sample = TestType()
    assert sample.method_with_return(0, 1) == (2, 0, 1)


def test_builtin_override():
    """ Ensures that when built-in methods are overridden, the overrides are properly executed """
    sample = TestType()
    assert str(sample) == 'string'
