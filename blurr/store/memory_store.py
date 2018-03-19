from typing import Any, Dict, List, Tuple

from blurr.core.store import Store, Key


class MemoryStore(Store):
    def __init__(self, state: Dict[str, Any] = None) -> None:
        super().__init__({'Name': 'memory', 'Type': 'MemoryStore'})
        if state:
            self.load(state)

    def load(self, state: Dict[str, Any]) -> Any:
        for k, v in state.items():
            key = Key.parse(k)
            self._cache[key] = v

    def _store_get_range(self, start: Key, end: Key = None,
                         count: int = 0) -> List[Tuple[Key, Any]]:
        if end is not None and end < start:
            temp = start
            start = end
            end = temp

        if not count:
            return [(key, item) for key, item in self._cache.items()
                    if start < key < end]
        else:
            filter_condition = (lambda i: i[0] > start) if count > 0 else (
                lambda i: i[0] < start)

            items = sorted(filter(filter_condition, list(self._cache.items())))

            if abs(count) > len(items):
                count = MemoryStore._sign(count) * len(items)

            if count < 0:
                return items[count:]
            else:
                return items[:count]

    @staticmethod
    def _sign(x: int) -> int:
        return (1, -1)[x < 0]

    def _store_get(self, key: Key):
        return None

    def _store_save(self, key: Key, item: Any) -> None:
        pass

    def _store_delete(self, key: Key) -> None:
        pass

    def _finalize(self) -> None:
        pass
