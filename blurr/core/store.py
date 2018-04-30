from abc import abstractmethod, ABC
from typing import Any, List, Tuple, Dict

from blurr.core.base import BaseSchema
from blurr.core.store_key import Key


class StoreSchema(BaseSchema):
    pass


class Store(ABC):
    """ Base Store that allows for data to be persisted during / after transformation """

    @abstractmethod
    def get_all(self, identity: str) -> Dict[Key, Any]:
        """
        Gets all the items for an identity
        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, key: Key) -> Any:
        """
        Gets an item by key
        """
        raise NotImplementedError()

    @abstractmethod
    def get_range(self, start: Key, end: Key = None, count: int = 0) -> List[Tuple[Key, Any]]:
        raise NotImplementedError()

    @abstractmethod
    def save(self, key: Key, item: Any) -> None:
        """
        Saves an item to store
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, key: Key) -> None:
        """
        Deletes an item from the store by key
        """
        raise NotImplementedError()

    @abstractmethod
    def finalize(self) -> None:
        """
        Finalizes the store by flushing all remaining data to persistence
        """
        raise NotImplementedError()
