from typing import Dict, Any

from blurr.core.store import Store


class LocalStore(Store):
    def __init__(self):
        self._dict_store: Dict[str, Any] = dict()
        super().__init__()

    def _store_get(self, identity, group):
        return self._dict_store.get(identity, {}).get(group, None)
