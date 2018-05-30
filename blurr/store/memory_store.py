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
        items = []
        for key, item in self._cache.items():
            if start < key < end:
                items.append((key, item))
        items = sorted(items)
        if count:
            items = self._restrict_items_to_count(items, count)
        return items

    def _get_range_dimension_key(self,
                                 base_key: Key,
                                 start_time: datetime,
                                 end_time: datetime,
                                 count: int = 0) -> List[Tuple[Key, Any]]:
        start_time_str = start_time.isoformat()
        end_time_str = end_time.isoformat()
        items = []
        for key, item in self._cache.items():
            if key.starts_with(base_key):
                item_time = item.get('_start_time', datetime.min.isoformat())
                if start_time_str < item_time < end_time_str:
                    items.append((key, item))
        items = sorted(items, key=lambda i: i[1].get('_start_time', datetime.min.isoformat()))
        if count:
            items = self._restrict_items_to_count(items, count)
        return items

    def save(self, key: Key, item: Any) -> None:
        self._cache[key] = item

    def delete(self, key: Key) -> None:
        self._cache.pop(key, None)

    def finalize(self) -> None:
        pass
