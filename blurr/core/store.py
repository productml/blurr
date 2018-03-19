from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List
from dateutil import parser


class Key:
    """
    A record in the store is identified by a key
    """
    PARTITION = '/'

    def __init__(self, identity: str, group: str,
                 timestamp: datetime = None) -> None:
        """
        Initializes a new key for storing data
        :param identity: Primary identity of the record being stored
        :param group: Secondary identity of the record
        :param timestamp: Optional timestamp that can be used for time range queries
        """
        if not identity or not group or identity.isspace() or group.isspace():
            raise ValueError('`identity` and `value` must be present.')

        self.identity = identity
        self.group = group
        self.timestamp = timestamp if not timestamp or timestamp.tzinfo else timestamp.replace(
            tzinfo=timezone.utc)

    @staticmethod
    def parse(key_string: str) -> 'Key':
        """ Parses a flat key string and returns a key """
        parts = key_string.split(Key.PARTITION)
        return Key(parts[0], parts[1],
                   parser.parse(parts[2]) if len(parts) > 2 else None)

    def __str__(self):
        """ Returns the string representation of the key"""
        if self.timestamp:
            return Key.PARTITION.join(
                [self.identity, self.group,
                 self.timestamp.isoformat()])

        return Key.PARTITION.join([self.identity, self.group])

    def __eq__(self, other: 'Key') -> bool:
        return (self.identity, self.group,
                self.timestamp) == (other.identity, other.group,
                                    other.timestamp)

    def __lt__(self, other: 'Key') -> bool:
        return (self.identity, self.group) == (
            other.identity, other.group) and self.timestamp < other.timestamp

    def __gt__(self, other: 'Key') -> bool:
        return (self.identity, self.group) == (
            other.identity, other.group) and self.timestamp > other.timestamp

    def __hash__(self):
        return hash((self.identity, self.group, self.timestamp))


class Store(ABC):
    """ Base Store that allows for data to be persisted during / after transformation """

    def __init__(self, spec: Dict[str, Any]) -> None:
        """
        Initializes the store based on the specifications
        """
        self.name = spec['Name']
        self.type = spec['Type']

    @abstractmethod
    def get(self, key: Key) -> Any:
        """
        Gets an item by key
        """
        pass

    @abstractmethod
    def get_range(self, start: Key, end: Key = None,
                  count: int = 0) -> Dict[Key, Any]:
        pass

    @abstractmethod
    def save(self, key: Key, item: Any) -> None:
        """
        Saves an item to store
        """
        pass

    @abstractmethod
    def delete(self, key: Key) -> None:
        """
        Deletes an item from the store by key
        """
        pass

    @abstractmethod
    def finalize(self) -> None:
        """
        Finalizes the store by flushing all remaining data to persistence
        """
        pass
