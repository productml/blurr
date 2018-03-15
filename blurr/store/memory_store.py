from typing import Any, Dict

from blurr.core.store import Store, Key


class MemoryStore(Store):
    def __init__(self):
        super().__init__({'Name': 'memory', 'Type': 'MemoryStore'})

    def _store_get_range(self, start: Key, end: Key, count: int = 0) -> Dict[Key, Any]:
        if not count:
            return {key: item for key, item in self._cache.items() if start < key < end}
        else:
            filter_condition = (lambda k, v: k > start) if count > 0 else (lambda k, v: k < start)

            items = sorted(filter(filter_condition, self._cache.items()))

            if abs(count) > len(items):
                count = (count/abs(count)) * len(items)

            if count < 0:
                return dict(items[count:])
            else:
                return dict(items[:count])


    def _store_get(self, key: Key):
        return None

    def _store_save(self, key: Key, item: Any) -> None:
        pass

    def _store_delete(self, key: Key) -> None:
        pass

    def _finalize(self) -> None:
        pass
