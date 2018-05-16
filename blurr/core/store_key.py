from datetime import datetime, timezone

from dateutil import parser


class Key:
    """
    A record in the store is identified by a key
    """
    PARTITION = '/'

    def __init__(self, identity: str, group: str, timestamp: datetime = None) -> None:
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

        self.identity = identity
        self.group = group
        self.timestamp = timestamp if not timestamp or timestamp.tzinfo else timestamp.replace(
            tzinfo=timezone.utc)

    @staticmethod
    def parse(key_string: str) -> 'Key':
        """ Parses a flat key string and returns a key """
        parts = key_string.split(Key.PARTITION)
        return Key(parts[0], parts[1], parser.parse(parts[2]) if len(parts) > 2 else None)

    def __str__(self):
        """ Returns the string representation of the key"""
        if self.timestamp:
            return Key.PARTITION.join([self.identity, self.group, self.timestamp.isoformat()])

        return Key.PARTITION.join([self.identity, self.group])

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other: 'Key') -> bool:
        return other and (self.identity, self.group,
                          self.timestamp) == (other.identity, other.group, other.timestamp)

    def __lt__(self, other: 'Key') -> bool:
        """
        Does a less than comparison on two keys. A None timestamp is considered
        larger than a timestamp that has been set.
        """
        if (self.identity, self.group) != (other.identity, other.group) or self.timestamp is None:
            return False

        return (other.timestamp is None) or (self.timestamp < other.timestamp)

    def __gt__(self, other: 'Key') -> bool:
        """
        Does a greater than comparison on two keys. A None timestamp is
        considered larger than a timestamp that has been set.
        """
        if (self.identity, self.group) != (other.identity, other.group) or other.timestamp is None:
            return False

        return (self.timestamp is None) or (self.timestamp > other.timestamp)

    def __hash__(self):
        return hash((self.identity, self.group, self.timestamp))
