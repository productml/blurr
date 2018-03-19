from typing import Any, Dict

from blurr.core.store import Store, Key


class MemoryStore(Store):
    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)

        self._cache: Dict[Key, Any] = dict()

    def get(self, key: Key) -> Any:
        return self._cache.get(key, None)

    def get_range(self, start: Key, end: Key = None,
                  count: int = 0) -> Dict[Key, Any]:
        if end and count:
            raise ValueError('Only one of `end` or `count` can be set')

        if not count:
            return {
                key: item
                for key, item in self._cache.items() if start < key < end
            }
        else:
            filter_condition = (lambda i: i[0] > start) if count > 0 else (
                lambda i: i[0] < start)

            items = sorted(filter(filter_condition, list(self._cache.items())))

            if abs(count) > len(items):
                count = (count / abs(count)) * len(items)

            if count < 0:
                return dict(items[count:])
            else:
                return dict(items[:count])

    def save(self, key: Key, item: Any) -> None:
        self._cache[key] = item

    def delete(self, key: Key) -> None:
        self._cache.pop(key, None)

    def finalize(self) -> None:
        pass
