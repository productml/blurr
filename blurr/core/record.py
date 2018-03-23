from typing import Dict, Any


class Record:
    """
    Wraps a dictionary into an object to allow dictionary keys to be accessed as object properties
    """

    def __init__(self, init: Dict[str, Any]) -> None:
        """
        Initializes a record object with a dictionary
        :param init: Dictionary to wrap as object
        """
        if not init:
            return

        for key, value in init.items():
            # Complex sub-objects are returned as sub-records
            if isinstance(value, dict):
                val = Record(value)
            # Lists are returned as Record lists
            elif isinstance(value, list) and len(value) > 0:
                val = RecordList(value)
            # Simple values are returned as-is
            else:
                val = value

            self.__dict__[key] = val

    def __getattr__(self, name):
        """
        When attributes are not found, None is returned
        """
        if not hasattr(super(), name):
            return None


class RecordList(list):
    """ Wraps a list to list of Records"""

    def __getitem__(self, item):
        value = super().__getitem__(item)

        if isinstance(value, dict):
            return Record(value)
            # Lists are returned as Record lists
        elif isinstance(value, list) and len(value) > 0:
            return RecordList(value)
            # Simple values are returned as-is

        return value
