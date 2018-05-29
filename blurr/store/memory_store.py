from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from dateutil import parser

from blurr.core.store import Store, Key, StoreSchema
from blurr.core.store_key import KeyType


class MemoryStoreSchema(StoreSchema):
    pass


class MemoryStore(Store):
    """
    In-memory store implementation
    """

    def __init__(self, schema: MemoryStoreSchema) -> None:
        self._schema = schema
        self._cache: Dict[Key, Any] = dict()

    def load(self):
        pass

    def get(self, key: Key) -> Any:
        return self._cache.get(key, None)

    def get_all(self, identity: str = None) -> Dict[Key, Any]:
        return {k: v
                for k, v in self._cache.items()
                if k.identity == identity} if identity else self._cache.copy()

    def _get_range_timestamp_key(self, start: Key, end: Key = None,
                                 count: int = 0) -> List[Tuple[Key, Any]]:
        if not count:
            items_in_range = []
            for key, item in self._cache.items():
                if start < key < end:
                    items_in_range.append((key, item))
            return items_in_range
        else:
            filter_condition = (lambda i: i[0] > start) if count > 0 else (lambda i: i[0] < start)

            items = sorted(filter(filter_condition, list(self._cache.items())))

            if abs(count) > len(items):
                count = MemoryStore._sign(count) * len(items)

            if count < 0:
                return items[count:]
            else:
                return items[:count]

    def _get_range_dimension_key(self,
                                 base_key: Key,
                                 start_time: datetime,
                                 end_time: datetime = None,
                                 count: int = 0) -> List[Tuple[Key, Any]]:
        pass

    @staticmethod
    def _sign(x: int) -> int:
        return (1, -1)[x < 0]

    def save(self, key: Key, item: Any) -> None:
        self._cache[key] = item

    def delete(self, key: Key) -> None:
        self._cache.pop(key, None)

    def finalize(self) -> None:
        pass
