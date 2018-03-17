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
        if identity.isspace() or group.isspace():
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
        self._cache: Dict[Key, Any] = dict()

    def prefetch(self, keys: List[Key]) -> None:
        """
        Pre-fetches items from the store and loads the cache
        :param keys: List of item keys
        """
        for key in keys:
            item = self._store_get(key)
            if item:
                self._cache[key] = item

    def get(self, key: Key) -> Any:
        """
        Gets an item by key
        """
        # Check the cache to see if item exists
        item = self._cache.get(key, None)

        if not item:
            # If not, load from store and put item in cache
            item = self._store_get(key)
            if item:
                self._cache[key] = item

        return item

    def get_range(self, start: Key, end: Key = None,
                  count: int = 0) -> Dict[Key, Any]:
        if end and count:
            raise ValueError('Only one of `end` or `count` can be set')

        result = self._store_get_range(start, end, count)
        for key, item in result.items():
            if key in self._cache:
                # If key is present in cache, use the cached value that may have been modified
                result[key] = self._cache[key]
            else:
                # Otherwise, add the new item to cache
                self._cache[key] = item

        return result

    @abstractmethod
    def _store_get_range(self, start: Key, end: Key = None,
                         count: int = 0) -> Dict[Key, Any]:
        pass

    @abstractmethod
    def _store_get(self, key: Key) -> Any:
        """
        Gets an item from the underlying store by the key
        """
        raise NotImplementedError()

    @abstractmethod
    def _store_save(self, key: Key, item: Any) -> None:
        """
        Saves an item by key to the underlying store
        """
        raise NotImplementedError()

    @abstractmethod
    def _store_delete(self, key: Key) -> None:
        """
        Deletes an item from the store by key
        """
        raise NotImplementedError()

    def save(self, key: Key, item: Any) -> None:
        """
        Saves an item to store
        """
        # Save to cache
        self._cache[key] = item

        # Save to the underlying store
        self._store_save(key, item)

    def delete(self, key: Key) -> None:
        """
        Deletes an item from the store by key
        """
        # Remove key from cache if it exists
        self._cache.pop(key, None)
        # Remove item from underlying store
        self._store_delete(key)

    @abstractmethod
    def _finalize(self) -> None:
        """
        Flushes all dirty items from the cache to the store
        """
        raise NotImplementedError()

    def close(self) -> None:
        """
        Closes the store by flushing all remaining data to persistence
        """

        self._finalize()
