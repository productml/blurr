from typing import Any


def wrap(value: Any) -> Any:
    if isinstance(value, dict):
        return Record(value)
    # Lists are returned as Record lists
    elif isinstance(value, list) and len(value) > 0:
        return RecordList(value)
    # Simple values are returned as-is
    return value


class Record(dict):
    """
    Wraps a dictionary into an object to allow dictionary keys to be accessed as object properties
    """

    def __getattr__(self, name):
        """
        When attributes are not found, None is returned
        """
        if name.startswith('__') and name.endswith('__'):
            raise AttributeError('Record object has no {} attribute.'.format(name))
        return wrap(self[name]) if name in self else None

    def __getitem__(self, item):
        return wrap(super().__getitem__(item)) if item in self else None


class RecordList(list):
    """ Wraps a list to list of Records"""

    def __getitem__(self, item):
        return wrap(super().__getitem__(item))
