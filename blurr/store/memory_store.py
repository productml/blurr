from typing import Dict, Any

from blurr.core.store import Store, Key


class MemoryStore(Store):

    def _store_get(self, key: Key):
        return None

    def _store_save(self, key: Key, item: Any) -> None:
        pass

    def _store_delete(self, key: Key) -> None:
        pass

    def _finalize(self) -> None:
        pass
