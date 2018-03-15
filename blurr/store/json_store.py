import json
from typing import Any, Dict, Set

from blurr.core.store import Store, Key


class JsonStore(Store):
    def __init__(self, spec: Dict[str, Any]) -> None:
        super().__init__(spec)
        self.file = spec['File']
        self.data = json.load(self.file)

    @staticmethod
    def _build_key(key: Key) -> str:
        return key.identity + "-" + key.group

    def _store_get(self, key: Key) -> Any:
        return self.data.get(self._build_key(key), None)

    def _store_save(self, key: Key, item: Any) -> None:
        self.data[self._build_key(key)] = item

    def _store_delete(self, key: Key) -> None:
        self.data.pop(self._build_key(key), None)

    def _finalize(self) -> None:
        json.dump(self.data, self.file)
