from abc import ABC, abstractmethod
from typing import Any, Dict


class Store(ABC):
    def __init__(self):
        # TODO Keep two caches - one that keeps the gets fresh from store
        # and one that keeps the saved states.  The initial and final dictionaries
        # are compared to see what needs to be flushed in the final final stage

        self._dict_store: Dict[str, Any] = dict()

    def _cache_get(self, identity: str, group: str) -> Any:
        return self._dict_store.get(identity, {}).get(group, None)

    def _cache_save(self, identity: str, group: str, value: Any) -> None:
        self._dict_store.setdefault(identity, {})[group] = value

    def prefetch(self, records):
        pass

    def get(self, identity: str, group: str) -> Any:
        item = self._cache_get(identity, group)
        if not item:
            item = self._store_get(identity, group)
            self._cache_save(item)

        return item

    @abstractmethod
    def _store_get(self, identity, group):
        pass

    def get_last(self, identity, group_id_prefix, count):
        pass

    def save(self, identity, group_id):
        pass

    def delete(self, identity, group_id):
        pass

    def flush(self):
        pass
