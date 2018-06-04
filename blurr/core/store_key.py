from datetime import datetime, timezone
from enum import Enum
from typing import List, Any

from dateutil import parser


class KeyType(Enum):
    DIMENSION = 1
    TIMESTAMP = 2
    UNKNOWN = 10


class Key:
    """
    A record in the store is identified by a key
    """
    PARTITION = '/'
    DIMENSION_PARTITION = ':'

    # TODO: Consider adding a * to force parameterization of attributes.
    def __init__(self,
                 key_type: KeyType,
                 identity: str,
                 group: str,
                 dimensions: List[str] = list(),
                 timestamp: datetime = None) -> None:
        """
        Initializes a new key for storing data
        :param identity: Primary identity of the record being stored
        :param group: Secondary identity of the record
        :param timestamp: Optional timestamp that can be used for time range queries
        """
        if not identity or identity.isspace():
            raise ValueError('`identity` must be present.')

        if not group or group.isspace():
            raise ValueError('`group` must be present.')

        if dimensions and timestamp:
            raise ValueError('Both dimensions and timestamp should not be set together.')

        if key_type == KeyType.DIMENSION and timestamp:
            raise ValueError('`timestamp` should not be set for KeyType.DIMENSION.')

        if key_type == KeyType.TIMESTAMP and dimensions:
            raise ValueError('`dimensions` should not be set for KeyType.TIMESTAMP.')

        self.key_type = key_type
        self.identity = identity
        self.group = group
        self.timestamp = timestamp if not timestamp or timestamp.tzinfo else timestamp.replace(
            tzinfo=timezone.utc)
        self.dimensions = dimensions

    # TODO: Handle '/' and ':' values in dimensions
    @property
    def dimensions_str(self):
        return ':'.join(self.dimensions) if self.dimensions else ''

    @staticmethod
    def parse(key_string: str) -> 'Key':
        """ Parses a flat key string and returns a key """
        parts = key_string.split(Key.PARTITION)
        key_type = KeyType.DIMENSION
        if parts[3]:
            key_type = KeyType.TIMESTAMP
        return Key(key_type, parts[0], parts[1], parts[2].split(Key.DIMENSION_PARTITION)
                   if parts[2] else [],
                   parser.parse(parts[3]) if parts[3] else None)

    @staticmethod
    def parse_sort_key(identity: str, sort_key_string: str) -> 'Key':
        """ Parses a flat key string and returns a key """
        parts = sort_key_string.split(Key.PARTITION)
        key_type = KeyType.DIMENSION
        if parts[2]:
            key_type = KeyType.TIMESTAMP
        return Key(key_type, identity, parts[0], parts[1].split(Key.DIMENSION_PARTITION)
                   if parts[1] else [],
                   parser.parse(parts[2]) if parts[2] else None)

    def __str__(self):
        """ Returns the string representation of the key"""
        return Key.PARTITION.join([self.identity, self.sort_key])

    @property
    def sort_key(self):
        return Key.PARTITION.join(
            [self.group, self.dimensions_str,
             self.timestamp.isoformat() if self.timestamp else ''])

    @property
    def sort_prefix_key(self):
        if self.key_type == KeyType.DIMENSION:
            return Key.PARTITION.join([self.group, self.dimensions_str]
                                      if self.dimensions_str else [self.group, ''])

        if self.key_type == KeyType.TIMESTAMP:
            return self.sort_key

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other: 'Key') -> bool:
        return other and (self.identity, self.group, self.timestamp,
                          self.dimensions) == (other.identity, other.group, other.timestamp,
                                               other.dimensions)

    def __lt__(self, other: 'Key') -> bool:
        """
        Does a less than comparison on two keys. A None timestamp is considered
        larger than a timestamp that has been set.
        """
        if (self.identity, self.group, self.key_type) != (other.identity, other.group,
                                                          other.key_type):
            return False

        if self.key_type == KeyType.TIMESTAMP:
            return self.timestamp < other.timestamp

        return self.dimensions < other.dimensions

    def __gt__(self, other: 'Key') -> bool:
        """
        Does a greater than comparison on two keys. A None timestamp is
        considered larger than a timestamp that has been set.
        """
        if (self.identity, self.group, self.key_type) != (other.identity, other.group,
                                                          other.key_type):
            return False

        if self.key_type == KeyType.TIMESTAMP:
            return self.timestamp > other.timestamp

        return self.dimensions > other.dimensions

    def __hash__(self):
        return hash((self.identity, self.group, self.timestamp, self.dimensions_str))

    def starts_with(self, other: 'Key') -> bool:
        """
        Checks if this key starts with the other key provided. Returns False if key_type, identity
        or group are different.
        For `KeyType.TIMESTAMP` returns True.
        For `KeyType.DIMENSION` does prefix match between the two dimensions property.
        """
        if (self.key_type, self.identity, self.group) != (other.key_type, other.identity,
                                                          other.group):
            return False
        if self.key_type == KeyType.TIMESTAMP:
            return True
        if self.key_type == KeyType.DIMENSION:
            if len(self.dimensions) < len(other.dimensions):
                return False
            return self.dimensions[0:len(other.dimensions)] == other.dimensions
