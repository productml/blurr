from abc import ABC, abstractmethod
from typing import Any, Dict, List, Set


class Key:
    """
    A record in the store is identified by a key
    """

    def __init__(self, identity: str, group: str) -> None:
        """
        A key is a composite of identity and group
        :param identity: Identifies the entity
        :param group: Identifies the data group
        """
        self.identity = identity
        self._group = group

    @property
    def group(self):
        """
        Returns the data group for the key
        """
        return self._group


class Store(ABC):
    def __init__(self, spec: Dict[str, Any]) -> None:
        """
        Initializes the store based on the specifications
        """
        self.name = spec['Name']
        self.type = spec['Type']
        self._cache: Dict[Key, Any] = dict()

    def _cache_get(self, key: Key) -> Any:
        """
        Gets a data item from the cache by key.  Returns None when no item is found in the cache.
        """
        return self._cache.get(key, None)

    def _cache_save(self, key: Key, item: Any) -> None:
        """
        Saves an item to the cache
        """
        self._cache[key] = item

    def prefetch(self, keys: List[Key]) -> None:
        """
        Pre-fetches items from the store and loads the cache
        :param keys: List of item keys
        """
        for key in keys:
            item = self._store_get(key)
            if item:
                self._cache_save(key, item)

    def get(self, key: Key) -> Any:
        """
        Gets an item by key
        """
        # Check the cache to see if item exists
        item = self._cache_get(key)

        if not item:
            # If not, load from store and put item in cache
            item = self._store_get(key)
            if item:
                self._cache_save(key, item)

        return item

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
        self._cache_save(key, item)

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
        return NotImplementedError()

    def close(self) -> None:
        """
        Closes the store by flushing all remaining data to persistence
        """

        self._finalize()
